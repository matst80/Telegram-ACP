use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use chrono::Utc;
use teloxide::prelude::*;
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

use crate::relay::{
    MultiSessionEventSink, SessionEvent, SessionEventSink,
};
use crate::session;
use crate::session_control::{self, SessionCommand};
use crate::session_log::{with_session_context, SessionContext, SessionLog};
use crate::types::{AgentEvent, SessionRecord, SessionStatus};
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
        // Create forum topic or assign negative thread ID for headless mode
        let (thread_id, is_telegram) = if let (Some(bot), Some(chat_id)) = (&self.bot, self.config.chat_id) {
            let topic = bot
                .create_forum_topic(ChatId(chat_id), &topic_name)
                .icon_color(teloxide::types::Rgb::from_u32(0x6FB9F0))
                .await?;
            (topic.thread_id.0 .0, true)
        } else {
            let headless_id = -1 - (self.session_manager.topics.len() as i32);
            (headless_id, false)
        };

        if let (Some(bot), Some(chat_id)) = (&self.bot, self.config.chat_id) {
            let _ = bot
                .send_message(
                    ChatId(chat_id),
                    format!("<b>Agent is starting up...</b>\n\nStarting ACP session in this topic for <code>{}</code>", crate::formatting::escape_html(&path)),
                )
                .message_thread_id(teloxide::types::ThreadId(teloxide::types::MessageId(thread_id)))
                .parse_mode(teloxide::types::ParseMode::Html)
                .await;
        }

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
                if is_telegram {
                    if let (Some(bot), Some(chat_id)) = (&self.bot, self.config.chat_id) {
                        let delete_result = bot
                            .delete_forum_topic(
                                ChatId(chat_id),
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
                        let _ = bot
                            .send_message(
                                ChatId(chat_id),
                                format!(
                                    "Failed to initialize ACP session for '{}' (topic {}). Topic was removed. Error:\n{:#}",
                                    path, thread_id, e
                                ),
                            )
                            .await;
                    }
                }
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
        if let (Some(bot), Some(chat_id)) = (&self.bot, self.config.chat_id) {
            let _ = bot
                .send_message(
                    ChatId(chat_id),
                    format!("<b>Agent is restarting...</b>\n\nRestoring ACP session in this topic for <code>{}</code>", crate::formatting::escape_html(&record.project_path.to_string_lossy())),
                )
                .message_thread_id(teloxide::types::ThreadId(teloxide::types::MessageId(thread_id)))
                .parse_mode(teloxide::types::ParseMode::Html)
                .await;
        }

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
        if let (Some(bot), Some(chat_id)) = (&self.bot, self.config.chat_id) {
            let _ = bot
                .reopen_forum_topic(
                    ChatId(chat_id),
                    teloxide::types::ThreadId(teloxide::types::MessageId(thread_id)),
                )
                .await;
        }

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
        let _resumed_session = existing_acp_session_id.is_some();

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
                self.config.chat_id.map(ChatId),
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
        let chat_id = self.config.chat_id.map(ChatId);
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
                            ctx = Some(crate::handlers::EventContext::for_telegram_opt(
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
            crate::daemon::agent_launcher::spawn_and_run_agent(
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
                self.config.chat_id.map(ChatId),
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
            .publish(SessionEvent::SessionStarted {
                thread_id: if thread_id > 0 { Some(thread_id) } else { None },
                acp_session_id: acp_session_id.clone(),
                name: topic_name.clone(),
                agent_name: agent_name.clone(),
                agent_command: agent_cmd.clone(),
                project_path: project_path.clone(),
                focus: initiated_via_switch,
            })
            .await;

        Ok(acp_session_id)
    }
}
