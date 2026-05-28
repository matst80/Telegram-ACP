use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use agent_client_protocol as acp_sdk;
use anyhow::Result;
use chrono::Utc;
use dashmap::DashMap;
use teloxide::prelude::*;
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

use crate::acp;
use crate::relay::{
    MultiSessionEventSink, SessionEvent, SessionEventSink,
};
use crate::session;
use crate::session_control::{self, SessionCommand};
use crate::session_log::{with_session_context, SessionContext, SessionLog};
use crate::types::{AgentEvent, SessionRecord, SessionStatus};
use crate::{sess_error, sess_info};
use crate::session_manager::TopicEntry;
use crate::daemon::{DaemonHandle, StartSessionRequest};

impl DaemonHandle {
    /// Spawn a new agent session: create topic, spawn agent, wire everything up.
    /// Waits for ACP init to complete before returning.
    pub async fn spawn_session(
        &self,
        path: String,
        _prompt: Option<String>,
        agent: Option<String>,
    ) -> Result<(String, i32)> {
        let project_path = PathBuf::from(&path);
        let (agent_name, agent_cmd) = self.config.resolve_agent(agent.as_deref())?;

        // Create forum topic
        let folder_name = project_path
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_else(|| path.clone());
        let topic_name = format!("{}: {}", folder_name, Self::generate_two_words());
        let topic = self
            .bot
            .create_forum_topic(ChatId(self.config.chat_id), &topic_name)
            .icon_color(teloxide::types::Rgb::from_u32(0x6FB9F0))
            .await?;
        let thread_id = topic.thread_id.0 .0;

        let _ = self
            .bot
            .send_message(
                ChatId(self.config.chat_id),
                format!("<b>Agent is starting up...</b>\n\nStarting ACP session in this topic for <code>{}</code>", crate::formatting::escape_html(&path)),
            )
            .message_thread_id(teloxide::types::ThreadId(teloxide::types::MessageId(thread_id)))
            .parse_mode(teloxide::types::ParseMode::Html)
            .await;

        let acp_session_id = match self
            .enqueue_start_session(
                thread_id,
                project_path,
                agent_cmd,
                Some(agent_name),
                None,
                false,
                Vec::new(),
            )
            .await
        {
            Ok(session_id) => session_id,
            Err(e) => {
                let delete_result = self
                    .bot
                    .delete_forum_topic(
                        ChatId(self.config.chat_id),
                        teloxide::types::ThreadId(teloxide::types::MessageId(thread_id)),
                    )
                    .await;
                if let Err(delete_err) = delete_result {
                    tracing::warn!(
                        "Failed to delete forum topic {} after ACP init failure: {}",
                        thread_id,
                        delete_err
                    );
                }
                let _ = self
                    .bot
                    .send_message(
                        ChatId(self.config.chat_id),
                        format!(
                            "Failed to initialize ACP session for '{}' (topic {}). Topic was removed. Error:\n{:#}",
                            path, thread_id, e
                        ),
                    )
                    .await;
                return Err(e);
            }
        };

        if let Some(text) = _prompt {
            if let Some(tx) = self
                .session_manager
                .get_session_command_tx_by_thread(thread_id)
            {
                let _ = tx.send(SessionCommand::Prompt(vec![
                    agent_client_protocol::ContentBlock::Text(
                        agent_client_protocol::TextContent::new(text),
                    ),
                ]));
            }
        }

        Ok((acp_session_id, thread_id))
    }

    /// Replace the active session in an existing topic with a new one.
    ///
    /// Initiated via the `/new` command in telegram, in one thread.
    pub async fn replace_session_in_thread(
        &self,
        thread_id: i32,
        project_path: PathBuf,
        agent: Option<String>,
    ) -> Result<String> {
        let (agent_name, agent_cmd) = self.config.resolve_agent(agent.as_deref())?;

        // Deactivate current session (drop it)
        if let Some(mut entry) = self.session_manager.topics.get_mut(&thread_id) {
            entry.active = None;
        }

        self.enqueue_start_session(
            thread_id,
            project_path,
            agent_cmd,
            Some(agent_name),
            None,
            false,
            Vec::new(),
        )
        .await
    }

    /// Switch to a previously used session in a topic by resuming it.
    pub async fn switch_to_session(
        &self,
        thread_id: i32,
        record: &SessionRecord,
    ) -> Result<String> {
        // Deactivate current session (drop it)
        if let Some(mut entry) = self.session_manager.topics.get_mut(&thread_id) {
            entry.active = None;
        }

        self.enqueue_start_session(
            thread_id,
            record.project_path.clone(),
            record.agent_command.clone(),
            record.agent_name.clone(),
            Some(record.acp_session_id.clone()),
            true,
            record.history.clone(),
        )
        .await
    }

    /// Restore a previously persisted session. Skips topic creation since the topic already exists.
    pub(crate) async fn restore_session(&self, thread_id: i32, record: &SessionRecord) -> Result<()> {
        let _ = self
            .bot
            .send_message(
                ChatId(self.config.chat_id),
                format!("<b>Agent is restarting...</b>\n\nRestoring ACP session in this topic for <code>{}</code>", crate::formatting::escape_html(&record.project_path.to_string_lossy())),
            )
            .message_thread_id(teloxide::types::ThreadId(teloxide::types::MessageId(thread_id)))
            .parse_mode(teloxide::types::ParseMode::Html)
            .await;

        let acp_session_id = self
            .enqueue_start_session(
                thread_id,
                record.project_path.clone(),
                record.agent_command.clone(),
                record.agent_name.clone(),
                Some(record.acp_session_id.clone()),
                false,
                record.history.clone(),
            )
            .await?;

        // Reopen the topic in case it was closed
        let _ = self
            .bot
            .reopen_forum_topic(
                ChatId(self.config.chat_id),
                teloxide::types::ThreadId(teloxide::types::MessageId(thread_id)),
            )
            .await;

        tracing::info!(
            "Restored session {} (thread {}, acp {})",
            record.project_path.display(),
            thread_id,
            acp_session_id
        );
        Ok(())
    }

    /// Common logic for starting a session (new or restored).
    pub async fn enqueue_start_session(
        &self,
        thread_id: i32,
        project_path: PathBuf,
        agent_cmd: String,
        agent_name: Option<String>,
        existing_acp_session_id: Option<String>,
        initiated_via_switch: bool,
        initial_history: Vec<SessionEvent>,
    ) -> Result<String> {
        let (result_tx, result_rx) = oneshot::channel();
        self.local_start_tx
            .send(StartSessionRequest {
                thread_id,
                project_path,
                agent_cmd,
                agent_name,
                existing_acp_session_id,
                initiated_via_switch,
                initial_history,
                result_tx,
            })
            .map_err(|_| anyhow::anyhow!("Daemon session starter is unavailable"))?;

        result_rx
            .await
            .map_err(|_| anyhow::anyhow!("Daemon session starter task exited"))?
    }

    /// Common logic for starting a session (new or restored).
    /// Spawns event consumer and agent task, waits for ACP init, inserts into DashMap.
    pub async fn start_session_local(
        &self,
        thread_id: i32,
        project_path: PathBuf,
        agent_cmd: String,
        agent_name: Option<String>,
        existing_acp_session_id: Option<String>,
        initiated_via_switch: bool,
        initial_history: Vec<SessionEvent>,
    ) -> Result<String> {
        // Channels
        let (command_tx, command_rx) = mpsc::unbounded_channel::<SessionCommand>();
        let (cancel_tx, cancel_rx) = mpsc::unbounded_channel::<oneshot::Sender<Result<()>>>();
        let (event_tx, event_rx) = mpsc::unbounded_channel::<AgentEvent>();
        let available_commands = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let (history_sink, history) =
            crate::relay::HistorySessionEventSink::new(self.config.websocket_history_limit);

        // Seed initial history
        {
            let mut h = history.lock().await;
            for event in Self::sanitize_history(initial_history) {
                h.push_back(event);
            }
        }

        let session_event_sink: Arc<dyn SessionEventSink> =
            Arc::new(MultiSessionEventSink::new(vec![
                self.session_event_sink.clone(),
                Arc::new(history_sink),
            ]));
        let resumed_session = existing_acp_session_id.is_some();

        let status = Arc::new(tokio::sync::Mutex::new(SessionStatus::Initializing));
        let session_log = SessionLog::new(
            Uuid::new_v4().to_string(),
            thread_id,
            project_path.clone(),
            agent_cmd.clone(),
            agent_name.clone(),
        )?;
        let session_context = SessionContext::new(session_log.clone());
        let mcp_session = Arc::new(
            crate::mcp::McpSession::new(
                self.bot.clone(),
                self.telegraph.clone(),
                ChatId(self.config.chat_id),
                if thread_id > 0 { Some(thread_id) } else { None },
                project_path.clone(),
                self.config.socket_path.clone(),
            )
            .await?,
        );
        let mcp_session_id = mcp_session.id.clone();
        let mcp_servers =
            crate::mcp::build_mcp_servers(&mcp_session_id, &self.config.socket_path, &self.config)?;

        // Spawn the event consumer within LocalSet.
        let bot = self.bot.clone();
        let chat_id = ChatId(self.config.chat_id);
        let thread_id_ref = Arc::new(std::sync::atomic::AtomicI32::new(thread_id));
        if thread_id > 0 {
            tokio::task::spawn_local(with_session_context(
                session_context.clone(),
                session::run_event_consumer(
                    bot,
                    chat_id,
                    thread_id,
                    event_rx,
                    available_commands.clone(),
                ),
            ));
        } else {
            let available_commands_clone = available_commands.clone();
            let bot_clone = bot.clone();
            let chat_id_clone = chat_id;
            let thread_id_ref_clone = thread_id_ref.clone();
            tokio::task::spawn_local(with_session_context(session_context.clone(), async move {
                let mut rx = event_rx;
                let mut ctx = None;
                let mut consumer = crate::handlers::SessionEventConsumer::new();

                while let Some(event) = rx.recv().await {
                    if let AgentEvent::Update(update) = &event {
                        if let agent_client_protocol::SessionUpdate::AvailableCommandsUpdate(u) =
                            update.as_ref()
                        {
                            *available_commands_clone.lock().await = u.available_commands.clone();
                        }
                    }

                    let current_tid =
                        thread_id_ref_clone.load(std::sync::atomic::Ordering::Relaxed);
                    if current_tid > 0 {
                        if ctx.is_none() {
                            ctx = Some(crate::handlers::EventContext::for_telegram(
                                bot_clone.clone(),
                                chat_id_clone,
                                current_tid,
                            ));
                        }
                        let c = ctx.as_mut().unwrap();
                        consumer.handle_event(&event, c).await;
                    }
                }
                if let Some(c) = ctx.as_mut() {
                    consumer.finish(c).await;
                }
            }));
        }

        // Dispatch to handlers
        let permission_handling = Arc::new(std::sync::Mutex::new(
            crate::types::PermissionHandling::Auto,
        ));

        let control_state = Arc::new(tokio::sync::Mutex::new(
            session_control::SessionControlState {
                current_permission_mode_id: None,
                permission_modes: Vec::new(),
                model_selector: None,
                permission_handling: crate::types::PermissionHandling::Auto,
            },
        ));
        // Get or create TopicEntry, set active
        let mut topic = self
            .session_manager
            .topics
            .entry(thread_id)
            .or_insert_with(|| TopicEntry {
                name: None,
                active: None,
                history: Vec::new(),
            });
        let topic_name = topic.name.clone();

        let session_entry = crate::session_manager::SessionEntry {
            acp_session_id: existing_acp_session_id.clone(), // preserve existing ID during resumption
            mcp_session_id: mcp_session_id.clone(),
            mcp: mcp_session.clone(),
            session_log: session_log.clone(),
            project_path: project_path.clone(),
            agent_command: agent_cmd.clone(),
            agent_name: agent_name.clone(),
            name: Arc::new(tokio::sync::Mutex::new(topic_name.clone())),
            status: status.clone(),
            available_commands: available_commands.clone(),
            control_state: control_state.clone(),
            permission_handling: permission_handling.clone(),
            command_tx: command_tx.clone(),
            cancel_tx: cancel_tx.clone(),
            history: history.clone(),
            event_sink: session_event_sink.clone(),
            telegram_thread_id: thread_id_ref,
        };
        topic.active = Some(session_entry);
        drop(topic);

        // Create oneshot for receiving the ACP session ID
        let (result_tx, result_rx) = oneshot::channel();

        // Spawn ACP init + session loop directly in LocalSet.
        tokio::task::spawn_local(with_session_context(
            session_context,
            spawn_and_run_agent(
                agent_cmd.clone(),
                project_path.clone(),
                session_log,
                event_tx,
                session_event_sink.clone(),
                command_rx,
                cancel_rx,
                status.clone(),
                control_state,
                permission_handling,
                self.pending_permissions.clone(),
                self.bot.clone(),
                ChatId(self.config.chat_id),
                thread_id,
                existing_acp_session_id,
                initiated_via_switch,
                mcp_servers,
                result_tx,
            ),
        ));

        // Wait for ACP init to complete, then fill in the real acp_session_id.
        let acp_session_id = result_rx.await??;
        if !self.session_manager.topics.contains_key(&thread_id) {
            tracing::info!("Topic {thread_id} was removed during ACP init, skipping session restoration/persistence");
            return Ok(acp_session_id);
        }
        if let Some(topic) = self.session_manager.topics.get(&thread_id) {
            if let Some(active) = topic.active.as_ref() {
                active
                    .session_log
                    .set_acp_session_id(acp_session_id.clone())?;
            }
        }
        if let Some(mut topic) = self.session_manager.topics.get_mut(&thread_id) {
            if let Some(active) = topic.active.as_mut() {
                active.acp_session_id = Some(acp_session_id.clone());
            }
        }

        // Build history record
        let now = Utc::now();
        let record = SessionRecord {
            acp_session_id: acp_session_id.clone(),
            project_path: project_path.clone(),
            agent_command: agent_cmd.clone(),
            agent_name: agent_name.clone(),
            created_at: now,
            last_updated_at: now,
            history: Vec::new(),
        };

        // Get or create TopicEntry, set active, append to history
        let mut topic = self
            .session_manager
            .topics
            .entry(thread_id)
            .or_insert_with(|| TopicEntry {
                name: None,
                active: None,
                history: Vec::new(),
            });
        // Check if this session already exists in history (resume case)
        let existing = topic
            .history
            .iter_mut()
            .find(|r| r.acp_session_id == record.acp_session_id);
        if let Some(existing) = existing {
            existing.last_updated_at = now;
            if existing.agent_name.is_none() {
                existing.agent_name = agent_name.clone();
            }
        } else {
            topic.history.push(record);
        }
        drop(topic);

        // Persist after successful init
        self.session_manager.persist_topics().await;

        session_event_sink
            .publish(if resumed_session && initiated_via_switch {
                SessionEvent::SessionSwitched {
                    thread_id: if thread_id > 0 { Some(thread_id) } else { None },
                    acp_session_id: acp_session_id.clone(),
                    name: topic_name.clone(),
                    agent_name: agent_name.clone(),
                    agent_command: agent_cmd.clone(),
                    project_path: project_path.clone(),
                }
            } else {
                SessionEvent::SessionStarted {
                    thread_id: if thread_id > 0 { Some(thread_id) } else { None },
                    acp_session_id: acp_session_id.clone(),
                    name: topic_name.clone(),
                    agent_name: agent_name.clone(),
                    agent_command: agent_cmd.clone(),
                    project_path: project_path.clone(),
                }
            })
            .await;

        Ok(acp_session_id)
    }
}

/// Init phase: spawn agent, initialize/resume ACP session, send result back via oneshot.
/// Run phase: enter session runtime (continues in same task).
#[allow(clippy::too_many_arguments)]
async fn spawn_and_run_agent(
    agent_cmd: String,
    project_path: PathBuf,
    session_log: Arc<SessionLog>,
    event_tx: mpsc::UnboundedSender<AgentEvent>,
    event_sink: Arc<dyn SessionEventSink>,
    command_rx: mpsc::UnboundedReceiver<SessionCommand>,
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
                    // On daemon restart, we send nothing
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

            // Send the ACP session ID back
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

            // Populate initial control state from bootstrap data
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

            // Run the session runtime
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

            // Clean up child process
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

/// Spawn agent subprocess, handle IO, and initialize or resume the ACP session.
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
    acp::SessionBootstrap,
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

    if let Some(ctx) = crate::session_log::try_current_session_context() {
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
        let session = acp::resume_session(
            &conn,
            project_path,
            old_id.clone(),
            mcp_servers,
            &session_log,
        )
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
        })?;
        session
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
