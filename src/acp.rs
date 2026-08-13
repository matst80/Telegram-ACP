pub use agent_client_protocol::*;
use agent_client_protocol as acp;
use anyhow::Result;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::sync::Mutex;
use teloxide::prelude::*;
use teloxide::types::{InlineKeyboardButton, InlineKeyboardMarkup, MessageId, ParseMode, ThreadId};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::sync::{mpsc, oneshot};
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};

use crate::formatting;
use crate::relay::{SessionEvent, SessionEventSink};
use crate::session_log::{SessionLog, TranscriptDirection};
use crate::types::{AgentEvent, PermissionHandling};

pub type SharedStderrTail = Arc<Mutex<VecDeque<String>>>;
const STDERR_TAIL_MAX_LINES: usize = 50;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CommandRequest {
    pub session_id: acp::SessionId,
    pub command_id: String,
    pub arguments: serde_json::Value,
}

impl CommandRequest {
    pub fn new(
        session_id: acp::SessionId,
        command_id: String,
        arguments: serde_json::Value,
    ) -> Self {
        Self {
            session_id,
            command_id,
            arguments,
        }
    }
}

#[async_trait::async_trait(?Send)]
pub trait CommandExt {
    async fn command(&self, req: CommandRequest) -> acp::Result<serde_json::Value>;
}

#[async_trait::async_trait(?Send)]
impl CommandExt for acp::ClientSideConnection {
    async fn command(&self, req: CommandRequest) -> acp::Result<serde_json::Value> {
        let params_json = serde_json::to_string(&req).map_err(|e| {
            acp::Error::new(acp::ErrorCode::InvalidParams.into(), e.to_string())
        })?;
        let params_raw = acp::RawValue::from_string(params_json).map_err(|e| {
            acp::Error::new(acp::ErrorCode::InvalidParams.into(), e.to_string())
        })?;
        
        let resp = self
            .ext_method(acp::ExtRequest::new(
                "command",
                params_raw.into(),
            ))
            .await?;
        
        serde_json::to_value(&resp.0).map_err(|e| {
            acp::Error::new(acp::ErrorCode::InternalError.into(), e.to_string())
        })
    }
}

pub struct SessionBootstrap {
    pub session_id: acp::SessionId,
    pub modes: Option<acp::SessionModeState>,
    pub config_options: Vec<acp::SessionConfigOption>,
}

/// Our ACP Client implementation that forwards agent notifications as AgentEvents.
pub struct TelegramClient {
    event_tx: mpsc::UnboundedSender<AgentEvent>,
    event_sink: Arc<dyn SessionEventSink>,
    session_log: Arc<SessionLog>,
    /// When true, session_notification is a no-op (suppresses replay during load).
    pub session_loading_in_progress: Arc<AtomicBool>,
    pub bot: Option<Bot>,
    pub chat_id: Option<ChatId>,
    pub thread_id: i32,
    pub permission_handling: Arc<Mutex<PermissionHandling>>,
    pub pending_permissions: Arc<DashMap<String, oneshot::Sender<acp::PermissionOptionId>>>,
}

impl TelegramClient {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        event_tx: mpsc::UnboundedSender<AgentEvent>,
        event_sink: Arc<dyn SessionEventSink>,
        session_log: Arc<SessionLog>,
        session_loading_in_progress: Arc<AtomicBool>,
        bot: Option<Bot>,
        chat_id: Option<ChatId>,
        thread_id: i32,
        permission_handling: Arc<Mutex<PermissionHandling>>,
        pending_permissions: Arc<DashMap<String, oneshot::Sender<acp::PermissionOptionId>>>,
    ) -> Self {
        Self {
            event_tx,
            event_sink,
            session_log,
            session_loading_in_progress,
            bot,
            chat_id,
            thread_id,
            permission_handling,
            pending_permissions,
        }
    }

    async fn send_event(&self, session_id: &acp::SessionId, event: AgentEvent) {
        let _ = self.event_tx.send(event.clone());
        self.event_sink
            .publish(SessionEvent::AgentUpdate {
                thread_id: if self.thread_id > 0 { Some(self.thread_id) } else { None },
                acp_session_id: session_id.to_string(),
                event,
            })
            .await;
    }
}

#[async_trait::async_trait(?Send)]
impl acp::Client for TelegramClient {
    async fn request_permission(
        &self,
        args: acp::RequestPermissionRequest,
    ) -> acp::Result<acp::RequestPermissionResponse> {
        let handling = {
            let h = self.permission_handling.lock().unwrap();
            *h
        };

        if handling == PermissionHandling::Auto {
            // Auto-approve: pick the first "allow" option, or first option if none are allow
            let option_id = args
                .options
                .iter()
                .find(|o| {
                    matches!(
                        o.kind,
                        acp::PermissionOptionKind::AllowAlways
                            | acp::PermissionOptionKind::AllowOnce
                    )
                })
                .or(args.options.first())
                .map(|o| o.option_id.clone())
                .unwrap_or_else(|| acp::PermissionOptionId::new("allow_always"));

            return Ok(acp::RequestPermissionResponse::new(
                acp::RequestPermissionOutcome::Selected(acp::SelectedPermissionOutcome::new(
                    option_id,
                )),
            ));
        }

        // Manual approval
        let request_id = uuid::Uuid::new_v4().to_string();
        let (tx, rx) = oneshot::channel();
        self.pending_permissions.insert(request_id.clone(), tx);

        let title = args
            .tool_call
            .fields
            .title
            .as_deref()
            .unwrap_or("Tool Call");

        self.event_sink.publish(SessionEvent::PermissionRequest {
            thread_id: if self.thread_id > 0 { Some(self.thread_id) } else { None },
            acp_session_id: args.session_id.to_string(),
            tool: title.to_string(),
            args: serde_json::to_value(&args.tool_call.fields.content).unwrap_or(serde_json::Value::Null),
            request_id: request_id.clone(),
        }).await;

        let sent_msg = if let (Some(bot), Some(chat_id)) = (&self.bot, self.chat_id) {
            if self.thread_id > 0 {
                let mut rows = Vec::new();
                for option in &args.options {
                    let label = option.name.clone();
                    let data = format!("approve:{}:{}", request_id, option.option_id.0);
                    rows.push(vec![InlineKeyboardButton::callback(label, data)]);
                }

                let keyboard = InlineKeyboardMarkup::new(rows);
                let content = args.tool_call.fields.content.as_deref().unwrap_or(&[]);
                let text = format!(
                    "<b>Permission Requested: {}</b>\n\n{}",
                    formatting::escape_html(title),
                    formatting::format_tool_content(content)
                );

                let sent = bot
                    .send_message(chat_id, text)
                    .message_thread_id(ThreadId(MessageId(self.thread_id)))
                    .parse_mode(ParseMode::Html)
                    .reply_markup(keyboard)
                    .await
                    .map_err(|e| {
                        acp::Error::new(acp::ErrorCode::InternalError.into(), format!("Failed to send permission request: {e}"))
                    })?;
                Some(sent)
            } else {
                None
            }
        } else {
            None
        };

        match rx.await {
            Ok(option_id) => {
                if let (Some(sent), Some(bot), Some(chat_id)) = (sent_msg, &self.bot, self.chat_id) {
                    let _ = bot.edit_message_reply_markup(chat_id, sent.id).await;
                }
                Ok(acp::RequestPermissionResponse::new(
                    acp::RequestPermissionOutcome::Selected(acp::SelectedPermissionOutcome::new(
                        option_id,
                    )),
                ))
            }
            Err(_) => {
                if let (Some(sent), Some(bot), Some(chat_id)) = (sent_msg, &self.bot, self.chat_id) {
                    let _ = bot
                        .edit_message_text(chat_id, sent.id, "Permission request cancelled or timed out.")
                        .await;
                }
                Err(acp::Error::new(acp::ErrorCode::InternalError.into(), "Permission request cancelled"))
            }
        }
    }

    async fn session_notification(&self, args: acp::SessionNotification) -> acp::Result<()> {
        if self.session_loading_in_progress.load(Ordering::Relaxed) {
            return Ok(());
        }

        let session_id = args.session_id;
        let update = args.update;
        if let Err(err) = self.session_log.log_acp_payload(
            TranscriptDirection::FromAgent,
            &serde_json::json!({
                "type": "session_notification",
                "session_id": &session_id,
                "update": &update,
            }),
        ) {
            tracing::warn!("Failed to record ACP notification: {err}");
        }

        match update {
            acp::SessionUpdate::AgentMessageChunk(_)
            | acp::SessionUpdate::AgentThoughtChunk(_)
            | acp::SessionUpdate::ToolCall(_)
            | acp::SessionUpdate::ToolCallUpdate(_)
            | acp::SessionUpdate::Plan(_)
            | acp::SessionUpdate::AvailableCommandsUpdate(_)
            | acp::SessionUpdate::UsageUpdate(_) => {
                self.send_event(&session_id, AgentEvent::Update(Box::new(update)))
                    .await
            }
            _ => {
                // Ignore other notification types (UserMessageChunk, mode/config updates, etc.)
            }
        }
        Ok(())
    }
}

/// Spawn an ACP agent subprocess and return the connection + child handle.
/// Must be called within a tokio LocalSet.
#[allow(clippy::too_many_arguments)]
pub fn spawn_agent(
    agent_cmd: &str,
    project_path: &Path,
    event_tx: mpsc::UnboundedSender<AgentEvent>,
    event_sink: Arc<dyn SessionEventSink>,
    session_log: Arc<SessionLog>,
    session_loading_in_progress: Arc<AtomicBool>,
    bot: Option<Bot>,
    chat_id: Option<ChatId>,
    thread_id: i32,
    permission_handling: Arc<Mutex<PermissionHandling>>,
    pending_permissions: Arc<DashMap<String, oneshot::Sender<acp::PermissionOptionId>>>,
) -> Result<(
    acp::ClientSideConnection,
    tokio::process::Child,
    SharedStderrTail,
    impl std::future::Future<Output = acp::Result<()>>,
)> {
    // Execute via shell so user-configured commands support shell expansion
    // (e.g. `~`, quoted args, and env interpolation) consistently with manual runs.
    let mut child = Command::new("bash")
        .arg("-lc")
        .arg(agent_cmd)
        .current_dir(project_path)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true)
        .spawn()?;

    let stdin = child.stdin.take().unwrap().compat_write();
    let stdout = child.stdout.take().unwrap().compat();
    let stderr = child.stderr.take().unwrap();

    let stderr_tail = spawn_stderr_drain(stderr, session_log.clone());

    let client = TelegramClient::new(
        event_tx,
        event_sink,
        session_log,
        session_loading_in_progress,
        bot,
        chat_id,
        thread_id,
        permission_handling,
        pending_permissions,
    );

    let (conn, handle_io) = acp::ClientSideConnection::new(client, stdin, stdout, |fut| {
        tokio::task::spawn_local(fut);
    });

    Ok((conn, child, stderr_tail, handle_io))
}

fn spawn_stderr_drain(
    stderr: tokio::process::ChildStderr,
    session_log: Arc<SessionLog>,
) -> SharedStderrTail {
    let stderr_tail: SharedStderrTail = Arc::new(Mutex::new(VecDeque::new()));
    let stderr_tail_for_task = Arc::clone(&stderr_tail);

    tokio::task::spawn_local(async move {
        let mut lines = BufReader::new(stderr).lines();
        loop {
            match lines.next_line().await {
                Ok(Some(line)) => {
                    if let Err(err) = session_log.write_agent_stderr_line(&line) {
                        tracing::warn!("Failed writing agent stderr line: {err}");
                    }
                    push_stderr_line(&stderr_tail_for_task, line);
                }
                Ok(None) => break,
                Err(err) => {
                    let message = format!("Failed reading agent stderr: {err}");
                    if let Err(write_err) = session_log.write_agent_stderr_line(&message) {
                        tracing::warn!("Failed writing agent stderr error: {write_err}");
                    }
                    break;
                }
            }
        }
    });

    stderr_tail
}

fn push_stderr_line(stderr_tail: &SharedStderrTail, line: String) {
    let mut tail = match stderr_tail.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    if tail.len() >= STDERR_TAIL_MAX_LINES {
        tail.pop_front();
    }
    tail.push_back(line);
}

pub fn format_stderr_tail(stderr_tail: &SharedStderrTail) -> String {
    let lines: Vec<String> = match stderr_tail.lock() {
        Ok(guard) => guard.iter().cloned().collect(),
        Err(poisoned) => poisoned.into_inner().iter().cloned().collect(),
    };
    if lines.is_empty() {
        String::new()
    } else {
        format!(
            "\nAgent stderr (last {} lines):\n{}",
            lines.len(),
            lines.join("\n")
        )
    }
}

/// Initialize an ACP connection: call initialize + new_session, return session_id.
pub async fn init_session(
    conn: &acp::ClientSideConnection,
    project_path: &Path,
    mcp_servers: Vec<acp::McpServer>,
    session_log: &SessionLog,
) -> Result<SessionBootstrap> {
    let init_request = acp::InitializeRequest::new(acp::ProtocolVersion::V1).client_info(
        acp::Implementation::new("telegram-acp", env!("CARGO_PKG_VERSION")).title("Telegram ACP"),
    );
    session_log.log_acp_payload(
        TranscriptDirection::ToAgent,
        &serde_json::json!({ "method": "initialize", "params": &init_request }),
    )?;
    let init_response = conn.initialize(init_request).await?;
    session_log.log_acp_payload(
        TranscriptDirection::FromAgent,
        &serde_json::json!({ "method": "initialize", "result": &init_response }),
    )?;

    let new_session_request = acp::NewSessionRequest::new(project_path).mcp_servers(mcp_servers);
    session_log.log_acp_payload(
        TranscriptDirection::ToAgent,
        &serde_json::json!({ "method": "new_session", "params": &new_session_request }),
    )?;
    let session_resp = conn.new_session(new_session_request).await?;
    session_log.log_acp_payload(
        TranscriptDirection::FromAgent,
        &serde_json::json!({ "method": "new_session", "result": &session_resp }),
    )?;

    Ok(SessionBootstrap {
        session_id: session_resp.session_id,
        modes: session_resp.modes,
        config_options: session_resp.config_options.unwrap_or_default(),
    })
}

/// Resume a previous ACP session using load_session if supported, otherwise fall back to new_session.
pub async fn resume_session(
    conn: &acp::ClientSideConnection,
    project_path: &Path,
    old_acp_session_id: String,
    mcp_servers: Vec<acp::McpServer>,
    session_log: &SessionLog,
) -> Result<SessionBootstrap> {
    let init_request = acp::InitializeRequest::new(acp::ProtocolVersion::V1).client_info(
        acp::Implementation::new("telegram-acp", env!("CARGO_PKG_VERSION")).title("Telegram ACP"),
    );
    session_log.log_acp_payload(
        TranscriptDirection::ToAgent,
        &serde_json::json!({ "method": "initialize", "params": &init_request }),
    )?;
    let init_resp = conn.initialize(init_request).await?;
    session_log.log_acp_payload(
        TranscriptDirection::FromAgent,
        &serde_json::json!({ "method": "initialize", "result": &init_resp }),
    )?;

    if init_resp.agent_capabilities.load_session {
        tracing::info!(
            "Agent supports load_session, resuming session {}",
            old_acp_session_id
        );
        let session_id = acp::SessionId::new(old_acp_session_id.clone());
        let load_request = acp::LoadSessionRequest::new(old_acp_session_id, project_path)
            .mcp_servers(mcp_servers.clone());
        session_log.log_acp_payload(
            TranscriptDirection::ToAgent,
            &serde_json::json!({ "method": "load_session", "params": &load_request }),
        )?;
        match conn.load_session(load_request).await {
            Ok(load_resp) => {
                session_log.log_acp_payload(
                    TranscriptDirection::FromAgent,
                    &serde_json::json!({ "method": "load_session", "result": &load_resp }),
                )?;
                // load_session succeeded — reuse the same session ID
                Ok(SessionBootstrap {
                    session_id,
                    modes: load_resp.modes,
                    config_options: load_resp.config_options.unwrap_or_default(),
                })
            }
            Err(e) => {
                tracing::warn!(
                    "load_session failed for {}, falling back to new_session: {e}",
                    session_id
                );
                let new_session_request =
                    acp::NewSessionRequest::new(project_path).mcp_servers(mcp_servers);
                session_log.log_acp_payload(
                    TranscriptDirection::ToAgent,
                    &serde_json::json!({ "method": "new_session", "params": &new_session_request }),
                )?;
                let session_resp = conn.new_session(new_session_request).await?;
                session_log.log_acp_payload(
                    TranscriptDirection::FromAgent,
                    &serde_json::json!({ "method": "new_session", "result": &session_resp }),
                )?;
                Ok(SessionBootstrap {
                    session_id: session_resp.session_id,
                    modes: session_resp.modes,
                    config_options: session_resp.config_options.unwrap_or_default(),
                })
            }
        }
    } else {
        tracing::info!("Agent does not support load_session, creating new session");
        let new_session_request =
            acp::NewSessionRequest::new(project_path).mcp_servers(mcp_servers);
        session_log.log_acp_payload(
            TranscriptDirection::ToAgent,
            &serde_json::json!({ "method": "new_session", "params": &new_session_request }),
        )?;
        let session_resp = conn.new_session(new_session_request).await?;
        session_log.log_acp_payload(
            TranscriptDirection::FromAgent,
            &serde_json::json!({ "method": "new_session", "result": &session_resp }),
        )?;
        Ok(SessionBootstrap {
            session_id: session_resp.session_id,
            modes: session_resp.modes,
            config_options: session_resp.config_options.unwrap_or_default(),
        })
    }
}
