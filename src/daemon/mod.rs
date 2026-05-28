use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use dashmap::DashMap;
use futures::future::join_all;
use telegraph_rs::Telegraph;
use teloxide::prelude::*;
use tokio::sync::{mpsc, oneshot};

use crate::config::Config;
use crate::relay::{
    BroadcastSessionEventSink, MultiSessionEventSink, NoopSessionEventSink, SessionEvent,
    SessionEventSink, SessionStateProvider,
};
use crate::terminal::TerminalManager;
use crate::session_manager::{SessionManager, TopicEntry};

pub mod session_starter;
pub mod command_handler;
pub mod directory_service;
pub mod mcp_handler;
pub mod rag;
pub mod utils;

/// Shared daemon state, accessible from Telegram handlers and IPC.
pub struct DaemonHandle {
    pub config: Config,
    pub bot: Bot,
    #[allow(dead_code)]
    pub telegraph: Arc<Telegraph>,
    pub session_event_sink: Arc<dyn SessionEventSink>,
    pub start_time: std::sync::atomic::AtomicI64,
    /// Relay for starting ACP sessions inside the daemon's LocalSet task.
    pub(crate) local_start_tx: mpsc::UnboundedSender<StartSessionRequest>,
    pub session_manager: SessionManager,
    pub terminal_manager: Arc<TerminalManager>,
    pub pending_permissions: Arc<DashMap<String, oneshot::Sender<agent_client_protocol::PermissionOptionId>>>,
    pub mdns: std::sync::Mutex<Option<mdns_sd::ServiceDaemon>>,
}

pub struct StartSessionRequest {
    pub(crate) thread_id: i32,
    pub(crate) project_path: PathBuf,
    pub(crate) agent_cmd: String,
    pub(crate) agent_name: Option<String>,
    pub(crate) existing_acp_session_id: Option<String>,
    /// Whether the session start request is initiated via `/switch` command in telegram.
    pub(crate) initiated_via_switch: bool,
    pub(crate) initial_history: Vec<SessionEvent>,
    pub(crate) result_tx: oneshot::Sender<Result<String>>,
}

impl DaemonHandle {
    pub async fn cancel_session(&self, thread_id: i32) -> Result<()> {
        let cancel_tx = {
            let entry = self
                .session_manager
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
        self.session_event_sink
            .publish(SessionEvent::SessionRemoved { thread_id })
            .await;
        Some(entry)
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
                let history = Self::sanitize_history(
                    active.history.lock().await.iter().cloned().collect(),
                );
                let acp_session_id = active.acp_session_id.clone().unwrap_or_default();
                let name = active.name.lock().await.clone();
                let available_commands = active.available_commands.lock().await.clone();
                sessions.push(crate::types::SessionInfo {
                    acp_session_id,
                    project_path: active.project_path.clone(),
                    status,
                    thread_id: Some(thread_id),
                    name,
                    agent_command: active.agent_command.clone(),
                    agent_name: active.agent_name.clone(),
                    available_commands,
                    history,
                });
            }
        }
        let projects = self.list_projects();
        let terminals = self.terminal_manager.list();
        vec![SessionEvent::Snapshot {
            sessions,
            rag_register_name: self.config.rag_register_name.clone(),
            projects,
            terminals,
        }]
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
        terminal_manager: Arc::new(TerminalManager::new(
            websocket_events
                .as_ref()
                .map(|sink| sink.clone() as Arc<dyn SessionEventSink>)
                .unwrap_or_else(|| Arc::new(NoopSessionEventSink)),
        )),
        pending_permissions: Arc::new(DashMap::new()),
        mdns: std::sync::Mutex::new(None),
    });

    if let Some(bind_addr) = config.websocket_bind.clone() {
        let websocket_events = websocket_events.expect("websocket sink missing");
        let state_provider = daemon.clone() as Arc<dyn SessionStateProvider>;
        let command_handler = daemon.clone() as Arc<dyn crate::relay::WebSocketCommandHandler>;
        if config.websocket_clipboard && config.global_clipboard_intercept {
            crate::clipboard::spawn_watcher(
                daemon.session_event_sink.clone(),
                config.websocket_clipboard_poll_ms,
                config.websocket_clipboard_max_bytes,
            );
        } else if config.websocket_clipboard {
            tracing::info!(
                "Global clipboard intercept disabled; websocket clipboard relay will only use non-global paths"
            );
        }
        if let Ok(addr) = bind_addr.parse::<std::net::SocketAddr>() {
            tracing::info!(bind_addr = %bind_addr, port = addr.port(), "Attempting to start mDNS advertising");
            match crate::websocket::advertise_service(addr.port()) {
                Ok(mdns_daemon) => {
                    tracing::info!(
                        port = addr.port(),
                        "Websocket mDNS advertising successfully registered in daemon"
                    );
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
                    crate::daemon::rag::run_rag_registration(config, port).await;
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
                        req.initial_history,
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
                                    let name = active.name.lock().await.clone();
                                    let available_commands =
                                        active.available_commands.lock().await.clone();
                                    sessions.push(crate::types::SessionInfo {
                                        acp_session_id,
                                        project_path: active.project_path.clone(),
                                        status,
                                        thread_id: if thread_id > 0 {
                                            Some(thread_id)
                                        } else {
                                            None
                                        },
                                        name,
                                        agent_command: active.agent_command.clone(),
                                        agent_name: active.agent_name.clone(),
                                        available_commands,
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
    let persisted = crate::persistence::load_topics();
    if !persisted.is_empty() {
        tracing::info!("Restoring {} persisted topic(s)", persisted.len());

        // Insert topic entries with history (active=None initially)
        for pt in &persisted {
            daemon.session_manager.topics.insert(
                pt.thread_id,
                TopicEntry {
                    name: pt.name.clone(),
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
    daemon.start_time.store(
        chrono::Utc::now().timestamp(),
        std::sync::atomic::Ordering::Relaxed,
    );

    crate::telegram::run_bot(bot, daemon).await;

    Ok(())
}
