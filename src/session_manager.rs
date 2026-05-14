use std::path::PathBuf;
use std::sync::Arc;
use dashmap::DashMap;
use tokio::sync::{mpsc, oneshot};
use agent_client_protocol as acp_sdk;
use crate::mcp;
use crate::session_log::SessionLog;
use crate::session_control::{self, SessionCommand};
use crate::types::{SessionRecord, SessionStatus};
use crate::relay::SessionEvent;
use anyhow::Result;

pub struct TopicEntry {
    pub name: Option<String>,
    /// Currently running session, if any.
    pub active: Option<SessionEntry>,
    /// All sessions ever bound to this topic.
    pub history: Vec<SessionRecord>,
}

pub struct SessionEntry {
    pub acp_session_id: Option<String>,
    pub mcp_session_id: String,
    pub mcp: Arc<mcp::McpSession>,
    pub session_log: Arc<SessionLog>,
    pub project_path: PathBuf,
    pub agent_command: String,
    pub agent_name: Option<String>,
    pub name: Arc<tokio::sync::Mutex<Option<String>>>,
    pub status: Arc<tokio::sync::Mutex<SessionStatus>>,
    pub available_commands: Arc<tokio::sync::Mutex<Vec<acp_sdk::AvailableCommand>>>,
    pub control_state: Arc<tokio::sync::Mutex<session_control::SessionControlState>>,
    #[allow(dead_code)]
    pub permission_handling: Arc<std::sync::Mutex<crate::types::PermissionHandling>>,
    pub command_tx: mpsc::UnboundedSender<SessionCommand>,
    pub cancel_tx: mpsc::UnboundedSender<oneshot::Sender<Result<()>>>,
    pub history: Arc<tokio::sync::Mutex<std::collections::VecDeque<SessionEvent>>>,
    pub event_sink: Arc<dyn crate::relay::SessionEventSink>,
    pub telegram_thread_id: Arc<std::sync::atomic::AtomicI32>,
}

pub struct SessionManager {
    pub topics: DashMap<i32, TopicEntry>,
}

impl SessionManager {
    pub fn new() -> Self {
        Self {
            topics: DashMap::new(),
        }
    }

    pub fn get_mcp_session_by_id(&self, session_id: &str) -> Option<Arc<crate::mcp::McpSession>> {
        self.topics.iter().find_map(|entry| {
            entry
                .active
                .as_ref()
                .filter(|s| s.mcp_session_id == session_id)
                .map(|s| s.mcp.clone())
        })
    }

    pub fn get_session_command_tx_by_thread(
        &self,
        thread_id: i32,
    ) -> Option<mpsc::UnboundedSender<SessionCommand>> {
        self.topics
            .get(&thread_id)?
            .active
            .as_ref()
            .map(|e| e.command_tx.clone())
    }

    pub fn get_session_project_path_by_thread(&self, thread_id: i32) -> Option<PathBuf> {
        self.topics
            .get(&thread_id)?
            .active
            .as_ref()
            .map(|e| e.project_path.clone())
    }

    pub fn get_session_agent_by_thread(&self, thread_id: i32) -> Option<String> {
        self.topics
            .get(&thread_id)?
            .active
            .as_ref()
            .and_then(|e| e.agent_name.clone())
    }

    pub fn get_acp_session_id_by_thread(&self, thread_id: i32) -> Option<String> {
        self.topics
            .get(&thread_id)?
            .active
            .as_ref()?
            .acp_session_id
            .clone()
    }

    pub async fn get_available_commands_by_thread(
        &self,
        thread_id: i32,
    ) -> Option<Vec<acp_sdk::AvailableCommand>> {
        let available_commands = self
            .topics
            .get(&thread_id)?
            .active
            .as_ref()
            .map(|e| e.available_commands.clone())?;
        let commands = available_commands.lock().await.clone();
        Some(commands)
    }

    pub fn get_session_event_sink_by_thread(
        &self,
        thread_id: i32,
    ) -> Option<Arc<dyn crate::relay::SessionEventSink>> {
        self.topics
            .get(&thread_id)?
            .active
            .as_ref()
            .map(|e| e.event_sink.clone())
    }

    pub fn resolve_session_tx(
        &self,
        thread_id: Option<i32>,
        session_id: Option<String>,
    ) -> Option<mpsc::UnboundedSender<SessionCommand>> {
        if let Some(sid) = session_id {
            for entry in self.topics.iter() {
                if let Some(active) = &entry.value().active {
                    if active.acp_session_id.as_deref() == Some(&sid) {
                        return Some(active.command_tx.clone());
                    }
                }
            }
        }
        if let Some(tid) = thread_id {
            return self.get_session_command_tx_by_thread(tid);
        }
        None
    }

    pub fn resolve_thread_id(
        &self,
        thread_id: Option<i32>,
        session_id: Option<String>,
    ) -> Option<i32> {
        if let Some(tid) = thread_id {
            return Some(tid);
        }
        if let Some(sid) = session_id {
            for entry in self.topics.iter() {
                if let Some(active) = &entry.value().active {
                    if active.acp_session_id.as_deref() == Some(&sid) {
                        return Some(*entry.key());
                    }
                }
            }
        }
        None
    }

    pub fn resolve_session_cancel_tx(
        &self,
        thread_id: Option<i32>,
        session_id: Option<String>,
    ) -> Option<mpsc::UnboundedSender<oneshot::Sender<Result<()>>>> {
        if let Some(sid) = session_id {
            for entry in self.topics.iter() {
                if let Some(active) = &entry.value().active {
                    if active.acp_session_id.as_deref() == Some(&sid) {
                        return Some(active.cancel_tx.clone());
                    }
                }
            }
        }
        if let Some(tid) = thread_id {
            return self.topics.get(&tid)?.active.as_ref().map(|e| e.cancel_tx.clone());
        }
        None
    }

    /// Remove a topic from in-memory state.
    pub async fn remove_topic(&self, thread_id: i32) -> Option<TopicEntry> {
        let (_, entry) = self.topics.remove(&thread_id)?;
        if let Some(active) = &entry.active {
            let (tx, _rx) = tokio::sync::oneshot::channel();
            let _ = active.cancel_tx.send(tx);
        }
        Some(entry)
    }

    /// Persist current topics to disk.
    pub async fn persist_topics(&self) {
        let mut persisted = Vec::new();
        for entry in self.topics.iter() {
            let thread_id = *entry.key();
            let topic = entry.value();
            let active_session_id = topic.active.as_ref().and_then(|s| s.acp_session_id.clone());
            persisted.push(crate::persistence::PersistedTopic {
                thread_id,
                name: topic.name.clone(),
                active_session_id,
                sessions: topic.history.clone(),
            });
        }
        if let Err(e) = crate::persistence::save_topics(&persisted) {
            tracing::error!("Failed to persist topics: {e}");
        }
    }
}
