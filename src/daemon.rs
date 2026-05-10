use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use agent_client_protocol as acp_sdk;
use anyhow::Result;
use chrono::Utc;
use dashmap::DashMap;
use futures::future::join_all;
use rmcp::service::RxJsonRpcMessage;
use rmcp::RoleServer;
use serde_json::Value as JsonValue;
use reqwest;
use telegraph_rs::Telegraph;
use teloxide::prelude::*;
use teloxide::types::InputFile;
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

use crate::acp;
use crate::config::Config;
use crate::mcp;
use crate::persistence;
use crate::relay::{
    BroadcastSessionEventSink, MultiSessionEventSink, NoopSessionEventSink, SessionEvent,
    SessionEventSink, SessionStateProvider,
};
use crate::session;
use crate::session_control::{self, SessionCommand};
use crate::session_log::{self, with_session_context, SessionContext, SessionLog};
use crate::telegram;
use crate::types::{AgentEvent, SessionRecord, SessionStatus};
use crate::{sess_error, sess_info};

use crate::session_manager::{SessionEntry, SessionManager, TopicEntry};

/// Shared daemon state, accessible from Telegram handlers and IPC.
pub struct DaemonHandle {
    pub config: Config,
    pub bot: Bot,
    #[allow(dead_code)]
    pub telegraph: Arc<Telegraph>,
    pub session_event_sink: Arc<dyn SessionEventSink>,
    pub start_time: std::sync::atomic::AtomicI64,
    /// Relay for starting ACP sessions inside the daemon's LocalSet task.
    local_start_tx: mpsc::UnboundedSender<StartSessionRequest>,
    pub session_manager: SessionManager,
    pub pending_permissions: Arc<DashMap<String, oneshot::Sender<acp_sdk::PermissionOptionId>>>,
    pub mdns: std::sync::Mutex<Option<mdns_sd::ServiceDaemon>>,
}

impl DaemonHandle {
    pub fn list_projects(&self) -> Vec<crate::relay::ProjectInfo> {
        let mut projects = Vec::new();
        if let Some(root) = &self.config.project_root {
            if let Ok(entries) = std::fs::read_dir(root) {
                for entry in entries.flatten() {
                    if let Ok(file_type) = entry.file_type() {
                        if file_type.is_dir() {
                            let path = entry.path();
                            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                                projects.push(crate::relay::ProjectInfo {
                                    name: name.to_string(),
                                    path,
                                });
                            }
                        }
                    }
                }
            }
        }
        projects.sort_by(|a, b| a.name.cmp(&b.name));
        projects
    }

    pub fn resolve_project_path(&self, path: PathBuf) -> PathBuf {
        if path.is_absolute() {
            return path;
        }

        if let Some(root) = &self.config.project_root {
            let joined = root.join(&path);
            if joined.exists() {
                return joined;
            }
        }
        path
    }
}

#[async_trait::async_trait]
impl SessionStateProvider for DaemonHandle {
    async fn get_snapshot(&self) -> Vec<SessionEvent> {
        let mut sessions = Vec::new();
        for entry in self.session_manager.topics.iter() {
            let thread_id = *entry.key();
            let topic = entry.value();
            if let Some(active) = &topic.active {
                let status = *active.status.lock().await;
                let history = active.history.lock().await.iter().cloned().collect();
                if let Some(acp_session_id) = active.acp_session_id.clone() {
                    sessions.push(crate::types::SessionInfo {
                        acp_session_id,
                        project_path: active.project_path.clone(),
                        agent_command: active.agent_command.clone(),
                        agent_name: active.agent_name.clone(),
                        status,
                        thread_id: if thread_id > 0 { Some(thread_id) } else { None },
                        history,
                    });
                }
            }
        }
        let projects = self.list_projects();
        vec![SessionEvent::Snapshot { sessions, projects }]
    }
}

#[async_trait::async_trait]
impl crate::relay::WebSocketCommandHandler for DaemonHandle {
    async fn handle_command(&self, command: crate::relay::WebSocketCommand) -> anyhow::Result<()> {
        match command {
            crate::relay::WebSocketCommand::SendPrompt { thread_id, session_id, text } => {
                let tx = self.session_manager
                    .resolve_session_tx(thread_id, session_id.clone())
                    .ok_or_else(|| anyhow::anyhow!("No active session found"))?;
                tx.send(SessionCommand::Prompt(vec![
                    agent_client_protocol::ContentBlock::Text(
                        agent_client_protocol::TextContent::new(text),
                    ),
                ]))
                .map_err(|_| anyhow::anyhow!("Failed to send prompt to session"))?;
            }
            crate::relay::WebSocketCommand::Cancel { thread_id, session_id } => {
                let cancel_tx = self.session_manager
                    .resolve_session_cancel_tx(thread_id, session_id)
                    .ok_or_else(|| anyhow::anyhow!("No active session found"))?;
                let (result_tx, result_rx) = tokio::sync::oneshot::channel();
                cancel_tx
                    .send(result_tx)
                    .map_err(|_| anyhow::anyhow!("Session cancel channel closed"))?;
                result_rx.await.map_err(|_| anyhow::anyhow!("Cancel request dropped"))??;
            }
            crate::relay::WebSocketCommand::SetConfigOption {
                thread_id,
                session_id,
                config_id,
                value_id,
            } => {
                let tx = self.session_manager
                    .resolve_session_tx(thread_id, session_id)
                    .ok_or_else(|| anyhow::anyhow!("No active session found"))?;
                let (result_tx, result_rx) = tokio::sync::oneshot::channel();
                tx.send(SessionCommand::SetConfigOption {
                    config_id,
                    value_id,
                    result_tx,
                })
                .map_err(|_| anyhow::anyhow!("Failed to send config change to session"))?;
                result_rx.await??;
            }
            crate::relay::WebSocketCommand::SetPermissionMode { thread_id, session_id, mode_id } => {
                let tx = self.session_manager
                    .resolve_session_tx(thread_id, session_id)
                    .ok_or_else(|| anyhow::anyhow!("No active session found"))?;
                let (result_tx, result_rx) = tokio::sync::oneshot::channel();
                tx.send(SessionCommand::SetPermissionMode { mode_id, result_tx })
                    .map_err(|_| anyhow::anyhow!("Failed to send permission change to session"))?;
                result_rx.await??;
            }
            crate::relay::WebSocketCommand::SpawnSession {
                project_path,
                agent_command,
                thread_id,
                _metadata: _,
            } => {
                let resolved_thread_id = thread_id.unwrap_or_else(|| {
                    // Generate unique negative thread ID for headless session
                    -1 - (self.session_manager.topics.len() as i32)
                });
                let path = self.resolve_project_path(PathBuf::from(&project_path));
                let (agent_name, agent_cmd) = self.config.resolve_agent(agent_command.as_deref())?;
                self.enqueue_start_session(
                    resolved_thread_id,
                    path,
                    agent_cmd,
                    Some(agent_name),
                    None,
                    false,
                )
                .await?;
            }
            crate::relay::WebSocketCommand::EndSession { session_id, thread_id } => {
                let cancel_tx = self.session_manager
                    .resolve_session_cancel_tx(thread_id, session_id.clone())
                    .ok_or_else(|| anyhow::anyhow!("No active session found"))?;
                let (result_tx, result_rx) = tokio::sync::oneshot::channel();
                let _ = cancel_tx.send(result_tx);
                let _ = result_rx.await;

                // Also remove it from topics if it's headless (negative ID)
                if let Some(tid) = thread_id {
                    if tid < 0 {
                        self.session_manager.topics.remove(&tid);
                    }
                } else if let Some(sid) = session_id {
                    let mut to_remove = None;
                    for entry in self.session_manager.topics.iter() {
                        if let Some(active) = &entry.value().active {
                            if active.acp_session_id.as_deref() == Some(&sid) {
                                to_remove = Some(*entry.key());
                                break;
                            }
                        }
                    }
                    if let Some(tid) = to_remove {
                        if tid < 0 {
                            self.session_manager.topics.remove(&tid);
                        }
                    }
                }
            }
            crate::relay::WebSocketCommand::PermissionResponse { request_id, decision } => {
                if let Some((_, tx)) = self.pending_permissions.remove(&request_id) {
                    let _ = tx.send(agent_client_protocol::PermissionOptionId::new(decision));
                }
            }
            crate::relay::WebSocketCommand::BindTelegramThread { session_id, thread_id, name } => {
                let sid = session_id.clone().ok_or_else(|| anyhow::anyhow!("session_id required"))?;
                
                // Validation rules (daemon-side)
                if thread_id.is_none() || thread_id.unwrap() <= 0 {
                    if let Some(n) = &name {
                        let trimmed = n.trim();
                        if trimmed.is_empty() {
                            self.session_event_sink.publish(SessionEvent::Error {
                                in_reply_to: Some("bind_telegram_thread".to_string()),
                                session_id: session_id.clone(),
                                code: "name_required".to_string(),
                                message: "name field required when thread_id is null".to_string(),
                            }).await;
                            return Ok(());
                        }
                        if trimmed.len() > 128 {
                            self.session_event_sink.publish(SessionEvent::Error {
                                in_reply_to: Some("bind_telegram_thread".to_string()),
                                session_id: session_id.clone(),
                                code: "invalid_argument".to_string(),
                                message: "name too long (max 128 chars)".to_string(),
                            }).await;
                            return Ok(());
                        }
                        if trimmed.chars().any(|c| c.is_control()) {
                            self.session_event_sink.publish(SessionEvent::Error {
                                in_reply_to: Some("bind_telegram_thread".to_string()),
                                session_id: session_id.clone(),
                                code: "invalid_argument".to_string(),
                                message: "name contains control characters".to_string(),
                            }).await;
                            return Ok(());
                        }
                    } else {
                        // Migration fallback
                        tracing::warn!("bind_telegram_thread: name missing when thread_id is null. Falling back to project name (deprecated).");
                    }
                }

                let mut old_key = None;
                for entry in self.session_manager.topics.iter() {
                    if let Some(active) = &entry.value().active {
                        if active.acp_session_id.as_deref() == Some(&sid) {
                            old_key = Some(*entry.key());
                            break;
                        }
                    }
                }
                let okey = old_key.ok_or_else(|| anyhow::anyhow!("No active session found"))?;

                let mut created = false;
                let (resolved_tid, resolved_name) = match thread_id {
                    Some(tid) if tid > 0 => (tid, name.clone().unwrap_or_default()),
                    _ => {
                        created = true;
                        let topic_name = if let Some(n) = name.as_ref() {
                            n.trim().to_string()
                        } else {
                            let folder_name = {
                                let topics = self.session_manager.topics.get(&okey).unwrap();
                                let active = topics.active.as_ref().unwrap();
                                active.project_path
                                    .file_name()
                                    .map(|n| n.to_string_lossy().to_string())
                                    .unwrap_or_else(|| "project".to_string())
                            };
                            format!("{}: {}", folder_name, Self::generate_two_words())
                        };
                        let topic = self.bot
                            .create_forum_topic(ChatId(self.config.chat_id), &topic_name)
                            .icon_color(teloxide::types::Rgb::from_u32(0x6FB9F0))
                            .await?;
                        (topic.thread_id.0 .0, topic_name)
                    }
                };

                if let Some((_, mut topic_entry)) = self.session_manager.topics.remove(&okey) {
                    if let Some(active) = topic_entry.active.take() {
                        active.telegram_thread_id.store(resolved_tid, std::sync::atomic::Ordering::Relaxed);
                        self.session_manager.topics.entry(resolved_tid).or_insert_with(|| TopicEntry {
                            active: None,
                            history: Vec::new(),
                        }).active = Some(active);

                        // Emit event
                        self.session_event_sink.publish(SessionEvent::TelegramThreadBound {
                            session_id: sid,
                            thread_id: resolved_tid,
                            name: resolved_name,
                            created,
                        }).await;
                    }
                }
            }
            crate::relay::WebSocketCommand::ListSessions => {
                use crate::relay::SessionStateProvider;
                let snapshot = self.get_snapshot().await;
                for event in snapshot {
                    self.session_event_sink.publish(event).await;
                }
            }
        }
        Ok(())
    }
}

pub struct StartSessionRequest {
    thread_id: i32,
    project_path: PathBuf,
    agent_cmd: String,
    agent_name: Option<String>,
    existing_acp_session_id: Option<String>,
    /// Whether the session start request is initiated via `/switch` command in telegram.
    initiated_via_switch: bool,
    result_tx: oneshot::Sender<Result<String>>,
}

impl DaemonHandle {
    pub async fn handle_mcp_message(
        &self,
        session_id: &str,
        payload: &str,
    ) -> Result<Option<String>> {
        tracing::debug!(session_id, "handle_mcp_message: looking up session");
        let mcp_session = self.session_manager
            .get_mcp_session_by_id(session_id)
            .ok_or_else(|| anyhow::anyhow!("Unknown MCP session id: {}", session_id))?;

        let payload_value: JsonValue = serde_json::from_str(payload)
            .map_err(|e| anyhow::anyhow!("Invalid MCP payload: {e}"))?;
        let expects_response = crate::mcp::mcp_expects_response(&payload_value);
        tracing::debug!(
            session_id,
            expects_response,
            payload = %payload,
            "handle_mcp_message: routing message"
        );
        let message: RxJsonRpcMessage<RoleServer> = serde_json::from_value(payload_value)
            .map_err(|e| anyhow::anyhow!("Failed to decode MCP payload: {e}"))?;

        mcp_session.send(message).await?;
        tracing::debug!(session_id, "handle_mcp_message: message sent to McpSession");

        if expects_response {
            tracing::debug!(session_id, "handle_mcp_message: waiting for response");
            if let Some(response) = mcp_session.next_response().await {
                let payload = serde_json::to_string(&response)?;
                tracing::debug!(session_id, response = %payload, "handle_mcp_message: got response");
                Ok(Some(payload))
            } else {
                tracing::warn!(
                    session_id,
                    "handle_mcp_message: outgoing channel closed while waiting for response"
                );
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    pub async fn cancel_session(&self, thread_id: i32) -> Result<()> {
        let cancel_tx = {
            let entry = self.session_manager
                .topics
                .get(&thread_id)
                .ok_or_else(|| anyhow::anyhow!("No topic for this thread"))?;
            let active = entry
                .active
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("No active session in this topic"))?;
            active.cancel_tx.clone()
        };
        let (result_tx, result_rx) = oneshot::channel();
        cancel_tx
            .send(result_tx)
            .map_err(|_| anyhow::anyhow!("Session cancel channel closed"))?;
        result_rx
            .await
            .map_err(|_| anyhow::anyhow!("Cancel request dropped"))?
    }

    /// Remove a topic from in-memory state and persisted storage.
    pub async fn remove_topic(&self, thread_id: i32) -> Option<TopicEntry> {
        let entry = self.session_manager.remove_topic(thread_id).await?;
        self.session_manager.persist_topics().await;
        Some(entry)
    }

    fn generate_two_words() -> String {
        const ADJECTIVES: &[&str] = &[
            "swift", "clever", "vibrant", "silent", "golden", "hyper", "stellar", "bright",
            "sleek", "agile", "zen", "iron", "quantum", "cyber", "rapid", "solar", "bold", "cool",
            "epic", "grand", "lunar", "neon", "prime", "sonic",
        ];
        const NOUNS: &[&str] = &[
            "eagle", "fox", "panda", "owl", "tiger", "wave", "storm", "pulse", "orbit", "spark",
            "edge", "forge", "core", "nexus", "link", "zenith", "hawk", "wolf", "lion", "bear",
            "crest", "flux", "nova", "shift",
        ];

        let u = uuid::Uuid::new_v4().as_u128();
        let adj = ADJECTIVES[(u & 0xFF) as usize % ADJECTIVES.len()];
        let noun = NOUNS[((u >> 8) & 0xFF) as usize % NOUNS.len()];
        format!("{} {}", adj, noun)
    }

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
            if let Some(tx) = self.session_manager.get_session_command_tx_by_thread(thread_id) {
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
        )
        .await
    }

    /// Restore a previously persisted session. Skips topic creation since the topic already exists.
    async fn restore_session(&self, thread_id: i32, record: &SessionRecord) -> Result<()> {
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
    async fn enqueue_start_session(
        &self,
        thread_id: i32,
        project_path: PathBuf,
        agent_cmd: String,
        agent_name: Option<String>,
        existing_acp_session_id: Option<String>,
        initiated_via_switch: bool,
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
                result_tx,
            })
            .map_err(|_| anyhow::anyhow!("Daemon session starter is unavailable"))?;

        result_rx
            .await
            .map_err(|_| anyhow::anyhow!("Daemon session starter task exited"))?
    }

    /// Common logic for starting a session (new or restored).
    /// Spawns event consumer and agent task, waits for ACP init, inserts into DashMap.
    async fn start_session_local(
        &self,
        thread_id: i32,
        project_path: PathBuf,
        agent_cmd: String,
        agent_name: Option<String>,
        existing_acp_session_id: Option<String>,
        initiated_via_switch: bool,
    ) -> Result<String> {
        // Channels
        let (command_tx, command_rx) = mpsc::unbounded_channel::<SessionCommand>();
        let (cancel_tx, cancel_rx) = mpsc::unbounded_channel::<oneshot::Sender<Result<()>>>();
        let (event_tx, event_rx) = mpsc::unbounded_channel::<AgentEvent>();
        let available_commands = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let (history_sink, history) =
            crate::relay::HistorySessionEventSink::new(self.config.websocket_history_limit);
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
            mcp::McpSession::new(
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
        let mcp_servers = crate::mcp::build_mcp_servers(&mcp_session_id, &self.config.socket_path, &self.config)?;

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
            tokio::task::spawn_local(with_session_context(
                session_context.clone(),
                async move {
                    let mut rx = event_rx;
                    let mut ctx = None;
                    let mut consumer = crate::handlers::SessionEventConsumer::new();

                    while let Some(event) = rx.recv().await {
                        if let AgentEvent::Update(update) = &event {
                            if let agent_client_protocol::SessionUpdate::AvailableCommandsUpdate(u) = update.as_ref() {
                                *available_commands_clone.lock().await = u.available_commands.clone();
                            }
                        }

                        let current_tid = thread_id_ref_clone.load(std::sync::atomic::Ordering::Relaxed);
                        if current_tid > 0 {
                            if ctx.is_none() {
                                ctx = Some(crate::handlers::EventContext::for_telegram(bot_clone.clone(), chat_id_clone, current_tid));
                            }
                            let c = ctx.as_mut().unwrap();
                            consumer.handle_event(&event, c).await;
                        }
                    }
                    if let Some(c) = ctx.as_mut() {
                        consumer.finish(c).await;
                    }
                },
            ));
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
        let session_entry = SessionEntry {
            acp_session_id: None, // filled in after init completes
            mcp_session_id: mcp_session_id.clone(),
            mcp: mcp_session.clone(),
            session_log: session_log.clone(),
            project_path: project_path.clone(),
            agent_command: agent_cmd.clone(),
            agent_name: agent_name.clone(),
            status: status.clone(),
            available_commands: available_commands.clone(),
            control_state: control_state.clone(),
            permission_handling: permission_handling.clone(),
            command_tx: command_tx.clone(),
            cancel_tx: cancel_tx.clone(),
            history: history.clone(),
            telegram_thread_id: thread_id_ref,
        };
        self.session_manager.topics
            .entry(thread_id)
            .or_insert_with(|| TopicEntry {
                active: None,
                history: Vec::new(),
            })
            .active = Some(session_entry);

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
            project_path,
            agent_command: agent_cmd,
            agent_name: agent_name.clone(),
            created_at: now,
            last_updated_at: now,
        };

        // Get or create TopicEntry, set active, append to history
        let mut topic = self.session_manager.topics.entry(thread_id).or_insert_with(|| TopicEntry {
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
                existing.agent_name = agent_name;
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
                }
            } else {
                SessionEvent::SessionStarted {
                    thread_id: if thread_id > 0 { Some(thread_id) } else { None },
                    acp_session_id: acp_session_id.clone(),
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
                        .send_document(chat_id, InputFile::file(stderr_path))
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

    if let Some(ctx) = session_log::try_current_session_context() {
        tokio::task::spawn_local(with_session_context(ctx, async move {
            if let Err(e) = handle_io.await {
                sess_error!("ACP IO error: {e}");
                let message = format!("Agent connection error: {e}");
                let _ = io_event_tx.send(AgentEvent::Error { content: message.clone() });
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
                let _ = io_event_tx.send(AgentEvent::Error { content: message.clone() });
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

async fn run_rag_registration(config: Config, actual_port: u16) {
    let url = match config.rag_register_url.as_ref() {
        Some(u) => u,
        None => return,
    };
    let token = match config.rag_token.as_ref() {
        Some(t) => t,
        None => {
            tracing::warn!("RAG registration URL provided but no token");
            return;
        }
    };
    let name = config
        .rag_register_name
        .clone()
        .unwrap_or_else(|| format!("acp-ws-{}", actual_port));
    let host = config
        .rag_register_host
        .clone()
        .unwrap_or_else(|| crate::websocket::get_local_ip());

    let client = reqwest::Client::new();

    // Heartbeat URL: replace /register with /heartbeat
    let heartbeat_url = url.replace("/register", "/heartbeat");

    let register_payload = serde_json::json!({
        "name": name,
        "host": host,
        "port": actual_port,
    });

    let heartbeat_payload = serde_json::json!({
        "name": name,
    });

    loop {
        tracing::info!(
            url = %url,
            name = %name,
            host = %host,
            port = actual_port,
            payload = ?register_payload,
            "Sending registration request to remote RAG"
        );
        let res = client
            .post(url)
            .header("Authorization", format!("Bearer {}", token))
            .json(&register_payload)
            .send()
            .await;

        match res {
            Ok(resp) if resp.status().is_success() => {
                tracing::info!("Successfully registered with remote RAG");

                // Heartbeat loop
                loop {
                    tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;
                    tracing::info!(
                        url = %heartbeat_url,
                        payload = ?heartbeat_payload,
                        "Sending heartbeat to remote RAG"
                    );
                    let res = client
                        .post(&heartbeat_url)
                        .header("Authorization", format!("Bearer {}", token))
                        .json(&heartbeat_payload)
                        .send()
                        .await;

                    match res {
                        Ok(resp) if resp.status().is_success() => {
                            let body = resp.text().await.unwrap_or_default();
                            if body.trim() == "false" {
                                tracing::warn!("RAG heartbeat returned false (not registered), re-registering");
                                break;
                            }
                            tracing::info!("RAG heartbeat sent successfully");
                        }
                        Ok(resp) => {
                            let status = resp.status();
                            let body = resp.text().await.unwrap_or_default();
                            tracing::warn!(
                                status = %status,
                                body = %body,
                                "RAG heartbeat failed, re-registering"
                            );
                            break; // break inner loop to re-register
                        }
                        Err(err) => {
                            tracing::warn!(error = %err, "RAG heartbeat error, re-registering");
                            break; // break inner loop to re-register
                        }
                    }
                }
            }
            Ok(resp) => {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                tracing::warn!(
                    status = %status,
                    body = %body,
                    "Failed to register with remote RAG, retrying in 30s"
                );
            }
            Err(err) => {
                tracing::warn!(
                    error = %err,
                    "Error registering with remote RAG, retrying in 30s"
                );
            }
        }

        tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
    }
}

/// Run the daemon: start bot + IPC listener.
pub async fn run_daemon(config: Config) -> Result<()> {
    tracing::info!("Starting telegram-acp daemon");

    let bot = Bot::new(&config.bot_token);
    let telegraph =
        Arc::new(crate::telegraph::create_account(config.telegraph_author.as_deref()).await?);
    let (local_start_tx, mut local_start_rx) = mpsc::unbounded_channel::<StartSessionRequest>();
    let websocket_events = config
        .websocket_bind
        .as_ref()
        .map(|_| Arc::new(BroadcastSessionEventSink::new(256)));
    let session_event_sink: Arc<dyn SessionEventSink> =
        if let Some(websocket_events) = websocket_events.as_ref() {
            Arc::new(MultiSessionEventSink::new(vec![
                websocket_events.clone() as Arc<dyn SessionEventSink>
            ]))
        } else {
            Arc::new(NoopSessionEventSink)
        };

    let daemon = Arc::new(DaemonHandle {
        config: config.clone(),
        bot: bot.clone(),
        telegraph,
        session_event_sink,
        start_time: std::sync::atomic::AtomicI64::new(chrono::Utc::now().timestamp()),
        local_start_tx,
        session_manager: SessionManager::new(),
        pending_permissions: Arc::new(DashMap::new()),
        mdns: std::sync::Mutex::new(None),
    });

    if let Some(bind_addr) = config.websocket_bind.clone() {
        let websocket_events = websocket_events.expect("websocket sink missing");
        let state_provider = daemon.clone() as Arc<dyn SessionStateProvider>;
        let command_handler = daemon.clone() as Arc<dyn crate::relay::WebSocketCommandHandler>;
        if let Ok(addr) = bind_addr.parse::<std::net::SocketAddr>() {
            tracing::info!(bind_addr = %bind_addr, port = addr.port(), "Attempting to start mDNS advertising");
            match crate::websocket::advertise_service(addr.port()) {
                Ok(mdns_daemon) => {
                    tracing::info!(port = addr.port(), "Websocket mDNS advertising successfully registered in daemon");
                    *daemon.mdns.lock().unwrap() = Some(mdns_daemon);
                }
                Err(err) => {
                    tracing::warn!("Failed to start mDNS advertising: {err}");
                }
            }

            if config.rag_register_url.is_some() {
                let config = config.clone();
                let port = addr.port();
                tokio::task::spawn_local(async move {
                    run_rag_registration(config, port).await;
                });
            }
        }

        tokio::task::spawn_local(async move {
            if let Err(err) = crate::websocket::run_server(
                &bind_addr,
                websocket_events,
                state_provider,
                command_handler,
            )
            .await
            {
                tracing::error!("Websocket server error: {err}");
            }
        });
    } else if config.rag_register_url.is_some() {
        tracing::warn!("RAG registration URL provided but websocket_bind is not set. RAG registration requires an active websocket server.");
    }

    let local_daemon = daemon.clone();
    tokio::task::spawn_local(async move {
        while let Some(req) = local_start_rx.recv().await {
            let local_daemon = local_daemon.clone();
            tokio::task::spawn_local(async move {
                let res = local_daemon
                    .start_session_local(
                        req.thread_id,
                        req.project_path,
                        req.agent_cmd,
                        req.agent_name,
                        req.existing_acp_session_id,
                        req.initiated_via_switch,
                    )
                    .await;
                let _ = req.result_tx.send(res);
            });
        }
    });

    // Spawn IPC server before restoring sessions so that mcp-relay subprocesses
    // spawned during load_session can connect to the socket immediately.
    let ipc_daemon = daemon.clone();
    let socket_path = config.socket_path.clone();
    tokio::task::spawn_local(async move {
        if let Err(e) = crate::ipc::run_ipc_server(&socket_path, move |cmd| {
            let daemon = ipc_daemon.clone();
            Box::pin(async move {
                use crate::types::{DaemonCommand, DaemonResponse};
                match cmd {
                    DaemonCommand::NewSession {
                        path,
                        prompt,
                        agent,
                    } => {
                        let path = daemon.resolve_project_path(path);
                        match daemon
                            .spawn_session(path.to_string_lossy().to_string(), prompt, agent)
                            .await
                        {
                            Ok((acp_session_id, thread_id)) => DaemonResponse::SessionCreated {
                                acp_session_id,
                                topic_url: format!(
                                    "https://t.me/c/{}/{}",
                                    daemon.config.chat_id, thread_id
                                ),
                            },
                            Err(e) => DaemonResponse::Error {
                                message: e.to_string(),
                            },
                        }
                    }
                    DaemonCommand::McpMessage {
                        session_id,
                        payload,
                    } => match daemon.handle_mcp_message(&session_id, &payload).await {
                        Ok(payload) => DaemonResponse::McpResponse { payload },
                        Err(e) => DaemonResponse::Error {
                            message: e.to_string(),
                        },
                    },
                    DaemonCommand::ListSessions => {
                        let mut sessions = Vec::new();
                        for entry in daemon.session_manager.topics.iter() {
                            let thread_id = *entry.key();
                            let topic = entry.value();
                            if let Some(active) = &topic.active {
                                let status = *active.status.lock().await;
                                let history = active.history.lock().await.iter().cloned().collect();
                                if let Some(acp_session_id) = active.acp_session_id.clone() {
                                    sessions.push(crate::types::SessionInfo {
                                        acp_session_id,
                                        project_path: active.project_path.clone(),
                                        agent_command: active.agent_command.clone(),
                                        agent_name: active.agent_name.clone(),
                                        status,
                                        thread_id: if thread_id > 0 { Some(thread_id) } else { None },
                                        history,
                                    });
                                }
                            }
                        }
                        DaemonResponse::SessionList { sessions }
                    }
                }
            })
        })
        .await
        {
            tracing::error!("IPC server error: {e}");
        }
    });

    // Restore persisted topics
    let persisted = persistence::load_topics();
    if !persisted.is_empty() {
        tracing::info!("Restoring {} persisted topic(s)", persisted.len());

        // Insert topic entries with history (active=None initially)
        for pt in &persisted {
            daemon.session_manager.topics.insert(
                pt.thread_id,
                TopicEntry {
                    active: None,
                    history: pt.sessions.clone(),
                },
            );
        }

        // Restore active sessions
        let restore_results = join_all(persisted.iter().filter_map(|pt| {
            let active_id = pt.active_session_id.as_ref()?;
            let record = pt
                .sessions
                .iter()
                .find(|r| &r.acp_session_id == active_id)?;
            let daemon = daemon.clone();
            let thread_id = pt.thread_id;
            let record = record.clone();
            Some(async move {
                let restore_result = daemon.restore_session(thread_id, &record).await;
                (thread_id, record, restore_result)
            })
        }))
        .await;

        for (thread_id, record, restore_result) in restore_results {
            if let Err(e) = restore_result {
                tracing::error!(
                    "Failed to restore session for {} (thread {}): {e}",
                    record.project_path.display(),
                    thread_id
                );
            }
        }
        // Re-persist to update any sessions that got new ACP IDs from fallback
        daemon.session_manager.persist_topics().await;
    }

    // Run Telegram bot (blocks)
    daemon.start_time.store(chrono::Utc::now().timestamp(), std::sync::atomic::Ordering::Relaxed);
    telegram::run_bot(bot, daemon).await;

    Ok(())
}
