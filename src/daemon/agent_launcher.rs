use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use agent_client_protocol as acp_sdk;
use anyhow::Result;
use dashmap::DashMap;
use teloxide::prelude::*;
use tokio::sync::{mpsc, oneshot};

use crate::acp::{self, SessionBootstrap};
use crate::relay::{SessionEvent, SessionEventSink};
use crate::session;
use crate::session_control;
use crate::session_log::{self, with_session_context, SessionLog};
use crate::types::{AgentEvent, SessionStatus};
use crate::{sess_error, sess_info};

#[allow(clippy::too_many_arguments)]
pub async fn spawn_and_run_agent(
    agent_cmd: String,
    project_path: PathBuf,
    session_log: Arc<SessionLog>,
    event_tx: mpsc::UnboundedSender<AgentEvent>,
    event_sink: Arc<dyn SessionEventSink>,
    command_rx: mpsc::UnboundedReceiver<session_control::SessionCommand>,
    cancel_rx: mpsc::UnboundedReceiver<oneshot::Sender<Result<()>>>,
    status: Arc<tokio::sync::Mutex<SessionStatus>>,
    control_state: Arc<tokio::sync::Mutex<session_control::SessionControlState>>,
    permission_handling: Arc<std::sync::Mutex<crate::types::PermissionHandling>>,
    pending_permissions: Arc<DashMap<String, oneshot::Sender<acp_sdk::PermissionOptionId>>>,
    bot: Bot,
    chat_id: ChatId,
    thread_id: i32,
    existing_acp_session_id: Option<String>,
    initiated_via_switch: bool,
    mcp_servers: Vec<acp_sdk::McpServer>,
    result_tx: oneshot::Sender<Result<String>>,
) {
    sess_info!(
        "ACP init started for project {} with agent {}",
        project_path.display(),
        agent_cmd
    );
    match init_agent(
        &agent_cmd,
        &project_path,
        session_log.clone(),
        event_tx.clone(),
        event_sink.clone(),
        &existing_acp_session_id,
        mcp_servers,
        bot.clone(),
        chat_id,
        thread_id,
        permission_handling.clone(),
        pending_permissions,
    )
    .await
    {
        Ok((conn, mut child, bootstrap, session_loading_in_progress)) => {
            sess_info!("ACP init completed with session {}", bootstrap.session_id);
            if existing_acp_session_id.is_some() {
                if initiated_via_switch {
                    let msg =
                        "Switched to the selected session. Replay hidden; ready for new prompts.";
                    let _ = bot
                        .send_message(chat_id, msg)
                        .message_thread_id(teloxide::types::ThreadId(teloxide::types::MessageId(
                            thread_id,
                        )))
                        .await;
                }
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                session_loading_in_progress.store(false, Ordering::Relaxed);
            }

            let session_id_str = bootstrap.session_id.to_string();
            if result_tx.send(Ok(session_id_str.clone())).is_err() {
                tracing::error!("Failed to send ACP session ID back (receiver dropped)");
                let _ = child.kill().await;
                return;
            }

            {
                let mut s = status.lock().await;
                *s = SessionStatus::Idle;
            }

            {
                let mut cs = control_state.lock().await;
                let handling = {
                    let h = permission_handling.lock().unwrap();
                    *h
                };
                *cs = session_control::build_control_state(
                    &bootstrap.modes,
                    &bootstrap.config_options,
                    handling,
                );
            }

            let conn = Arc::new(conn);
            session::run_session_runtime(
                conn,
                bootstrap.session_id,
                bot,
                chat_id,
                thread_id,
                command_rx,
                cancel_rx,
                event_tx,
                event_sink.clone(),
                status,
                control_state,
                permission_handling,
                bootstrap.modes,
                bootstrap.config_options,
            )
            .await;

            let _ = child.kill().await;
            event_sink
                .publish(SessionEvent::SessionEnded {
                    thread_id: if thread_id > 0 { Some(thread_id) } else { None },
                    acp_session_id: Some(session_id_str),
                })
                .await;
            sess_info!("Session runtime finished");
        }
        Err(e) => {
            sess_error!(
                "Failed to initialize ACP agent (cmd: {}, project: {}): {:#}",
                agent_cmd,
                project_path.display(),
                e
            );
            tracing::error!(
                "Failed to initialize ACP agent (cmd: {}, project: {}): {:#}",
                agent_cmd,
                project_path.display(),
                e
            );
            let stderr_path = session_log.agent_stderr_path();
            if stderr_path.exists() {
                let metadata = std::fs::metadata(&stderr_path).ok();
                if metadata.map(|m| m.len() > 0).unwrap_or(false) {
                    let _ = bot
                        .send_document(chat_id, teloxide::types::InputFile::file(stderr_path))
                        .caption("Agent stderr log")
                        .message_thread_id(teloxide::types::ThreadId(teloxide::types::MessageId(
                            thread_id,
                        )))
                        .await;
                }
            }
            let _ = result_tx.send(Err(e));
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn init_agent(
    agent_cmd: &str,
    project_path: &std::path::Path,
    session_log: Arc<SessionLog>,
    event_tx: mpsc::UnboundedSender<AgentEvent>,
    event_sink: Arc<dyn SessionEventSink>,
    existing_acp_session_id: &Option<String>,
    mcp_servers: Vec<acp_sdk::McpServer>,
    bot: Bot,
    chat_id: ChatId,
    thread_id: i32,
    permission_handling: Arc<std::sync::Mutex<crate::types::PermissionHandling>>,
    pending_permissions: Arc<DashMap<String, oneshot::Sender<acp_sdk::PermissionOptionId>>>,
) -> Result<(
    agent_client_protocol::ClientSideConnection,
    tokio::process::Child,
    SessionBootstrap,
    Arc<AtomicBool>,
)> {
    let session_loading_in_progress = Arc::new(AtomicBool::new(existing_acp_session_id.is_some()));
    let io_event_tx = event_tx.clone();
    let io_event_sink = event_sink.clone();
    let (session_id_tx, session_id_rx) =
        tokio::sync::watch::channel(existing_acp_session_id.clone());

    let (conn, child, stderr_tail, handle_io) = acp::spawn_agent(
        agent_cmd,
        project_path,
        event_tx,
        event_sink,
        session_log.clone(),
        session_loading_in_progress.clone(),
        bot,
        chat_id,
        thread_id,
        permission_handling,
        pending_permissions,
    )
    .map_err(|e| {
        anyhow::anyhow!(
            "failed to spawn ACP agent process (cmd: {}, project: {}): {:#}",
            agent_cmd,
            project_path.display(),
            e
        )
    })?;

    if let Some(ctx) = session_log::try_current_session_context() {
        tokio::task::spawn_local(with_session_context(ctx, async move {
            if let Err(e) = handle_io.await {
                sess_error!("ACP IO error: {e}");
                let message = format!("Agent connection error: {e}");
                let _ = io_event_tx.send(AgentEvent::Error {
                    content: message.clone(),
                });
                if let Some(acp_session_id) = session_id_rx.borrow().clone() {
                    io_event_sink
                        .publish(SessionEvent::AgentUpdate {
                            thread_id: if thread_id > 0 { Some(thread_id) } else { None },
                            acp_session_id,
                            event: AgentEvent::Error { content: message },
                        })
                        .await;
                }
            }
        }));
    } else {
        tokio::task::spawn_local(async move {
            if let Err(e) = handle_io.await {
                tracing::error!("ACP IO error: {e}");
                let message = format!("Agent connection error: {e}");
                let _ = io_event_tx.send(AgentEvent::Error {
                    content: message.clone(),
                });
                if let Some(acp_session_id) = session_id_rx.borrow().clone() {
                    io_event_sink
                        .publish(SessionEvent::AgentUpdate {
                            thread_id: if thread_id > 0 { Some(thread_id) } else { None },
                            acp_session_id,
                            event: AgentEvent::Error { content: message },
                        })
                        .await;
                }
            }
        });
    }

    let bootstrap = if let Some(old_id) = existing_acp_session_id.clone() {
        acp::resume_session(&conn, project_path, old_id.clone(), mcp_servers, &session_log)
            .await
            .map_err(|e| {
                let stderr_tail = acp::format_stderr_tail(&stderr_tail);
                anyhow::anyhow!(
                    "ACP resume_session failed (cmd: {}, project: {}, previous_session: {}): {:#}{}",
                    agent_cmd,
                    project_path.display(),
                    old_id,
                    e,
                    stderr_tail
                )
            })?
    } else {
        acp::init_session(&conn, project_path, mcp_servers, &session_log)
            .await
            .map_err(|e| {
                let stderr_tail = acp::format_stderr_tail(&stderr_tail);
                anyhow::anyhow!(
                    "ACP init_session failed (cmd: {}, project: {}): {:#}{}",
                    agent_cmd,
                    project_path.display(),
                    e,
                    stderr_tail
                )
            })?
    };
    let _ = session_id_tx.send(Some(bootstrap.session_id.to_string()));

    Ok((conn, child, bootstrap, session_loading_in_progress))
}