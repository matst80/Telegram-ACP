use std::collections::VecDeque;
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, mpsc, Mutex};

use crate::types::AgentEvent;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectInfo {
    pub name: String,
    pub path: std::path::PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SessionEvent {
    UserPrompt {
        thread_id: Option<i32>,
        acp_session_id: Option<String>,
        text: String,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        content: Vec<agent_client_protocol::ContentBlock>,
    },
    AgentUpdate {
        thread_id: Option<i32>,
        acp_session_id: String,
        event: AgentEvent,
    },
    SessionStarted {
        thread_id: Option<i32>,
        acp_session_id: String,
    },
    SessionSwitched {
        thread_id: Option<i32>,
        acp_session_id: String,
    },
    SessionEnded {
        thread_id: Option<i32>,
        acp_session_id: Option<String>,
    },
    SessionRemoved {
        thread_id: i32,
    },
    PermissionRequest {
        thread_id: Option<i32>,
        acp_session_id: String,
        tool: String,
        args: serde_json::Value,
        request_id: String,
    },
    TelegramThreadBound {
        session_id: String,
        thread_id: i32,
        name: String,
        created: bool,
    },
    Error {
        #[serde(skip_serializing_if = "Option::is_none")]
        in_reply_to: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        session_id: Option<String>,
        code: String,
        message: String,
    },
    SessionRenamed {
        thread_id: i32,
        acp_session_id: String,
        name: String,
    },
    TopicRemoved {
        thread_id: i32,
    },
    Snapshot {
        sessions: Vec<crate::types::SessionInfo>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        projects: Vec<ProjectInfo>,
    },
}

#[async_trait]
pub trait SessionEventSink: Send + Sync {
    async fn publish(&self, event: SessionEvent);
}

#[async_trait]
pub trait SessionStateProvider: Send + Sync {
    async fn get_snapshot(&self) -> Vec<SessionEvent>;
}

#[derive(Debug, serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WebSocketCommand {
    SendPrompt {
        #[serde(default)]
        thread_id: Option<i32>,
        #[serde(default)]
        session_id: Option<String>,
        text: String,
    },
    Cancel {
        #[serde(default)]
        thread_id: Option<i32>,
        #[serde(default)]
        session_id: Option<String>,
    },
    SetConfigOption {
        #[serde(default)]
        thread_id: Option<i32>,
        #[serde(default)]
        session_id: Option<String>,
        config_id: String,
        value_id: String,
    },
    SetPermissionMode {
        #[serde(default)]
        thread_id: Option<i32>,
        #[serde(default)]
        session_id: Option<String>,
        mode_id: String,
    },
    SpawnSession {
        project_path: String,
        #[serde(default, alias = "agent")]
        agent_command: Option<String>,
        #[serde(default)]
        thread_id: Option<i32>,
        #[serde(default)]
        _metadata: Option<serde_json::Value>,
    },
    EndSession {
        #[serde(default)]
        session_id: Option<String>,
        #[serde(default)]
        thread_id: Option<i32>,
    },
    PermissionResponse {
        request_id: String,
        decision: String,
    },
    BindTelegramThread {
        #[serde(default)]
        session_id: Option<String>,
        #[serde(default)]
        thread_id: Option<i32>,
        #[serde(default)]
        name: Option<String>,
    },
    RenameSession {
        #[serde(default)]
        thread_id: Option<i32>,
        #[serde(default)]
        session_id: Option<String>,
        name: String,
    },
    RemoveTopic {
        thread_id: i32,
    },
    ExecuteCommand {
        #[serde(default)]
        thread_id: Option<i32>,
        #[serde(default)]
        session_id: Option<String>,
        command_id: String,
        #[serde(default)]
        arguments: serde_json::Value,
    },
    ListSessions,
}

#[async_trait]
pub trait WebSocketCommandHandler: Send + Sync {
    async fn handle_command(&self, command: WebSocketCommand) -> anyhow::Result<()>;
}

pub struct NoopSessionEventSink;

#[async_trait]
impl SessionEventSink for NoopSessionEventSink {
    async fn publish(&self, _event: SessionEvent) {}
}

pub struct MultiSessionEventSink {
    sinks: Vec<Arc<dyn SessionEventSink>>,
}

impl MultiSessionEventSink {
    pub fn new(sinks: Vec<Arc<dyn SessionEventSink>>) -> Self {
        Self { sinks }
    }
}

#[async_trait]
impl SessionEventSink for MultiSessionEventSink {
    async fn publish(&self, event: SessionEvent) {
        for sink in &self.sinks {
            sink.publish(event.clone()).await;
        }
    }
}

pub struct HistorySessionEventSink {
    history: Arc<Mutex<VecDeque<SessionEvent>>>,
    limit: usize,
}

impl HistorySessionEventSink {
    pub fn new(limit: usize) -> (Self, Arc<Mutex<VecDeque<SessionEvent>>>) {
        let history = Arc::new(Mutex::new(VecDeque::with_capacity(limit)));
        (
            Self {
                history: history.clone(),
                limit,
            },
            history,
        )
    }
}

#[async_trait]
impl SessionEventSink for HistorySessionEventSink {
    async fn publish(&self, event: SessionEvent) {
        // Don't record snapshots in history
        if matches!(event, SessionEvent::Snapshot { .. }) {
            return;
        }

        let mut history = self.history.lock().await;
        if history.len() >= self.limit {
            history.pop_front();
        }
        history.push_back(event);
    }
}

#[allow(dead_code)]
pub struct ChannelSessionEventSink {
    tx: mpsc::UnboundedSender<SessionEvent>,
}

#[allow(dead_code)]
impl ChannelSessionEventSink {
    pub fn new(tx: mpsc::UnboundedSender<SessionEvent>) -> Self {
        Self { tx }
    }
}

#[async_trait]
impl SessionEventSink for ChannelSessionEventSink {
    async fn publish(&self, event: SessionEvent) {
        let _ = self.tx.send(event);
    }
}

pub struct BroadcastSessionEventSink {
    tx: broadcast::Sender<String>,
}

impl BroadcastSessionEventSink {
    pub fn new(capacity: usize) -> Self {
        let (tx, _) = broadcast::channel(capacity);
        Self { tx }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<String> {
        self.tx.subscribe()
    }
}

#[async_trait]
impl SessionEventSink for BroadcastSessionEventSink {
    async fn publish(&self, event: SessionEvent) {
        match serde_json::to_string(&event) {
            Ok(payload) => {
                let _ = self.tx.send(payload);
            }
            Err(err) => {
                tracing::warn!("Failed to serialize session event: {err}");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::Mutex;

    struct MockEventSink {
        events: Arc<Mutex<Vec<SessionEvent>>>,
    }

    #[async_trait]
    impl SessionEventSink for MockEventSink {
        async fn publish(&self, event: SessionEvent) {
            self.events.lock().await.push(event);
        }
    }

    #[tokio::test]
    async fn multi_sink_fans_out_events() {
        let list1 = Arc::new(Mutex::new(Vec::new()));
        let list2 = Arc::new(Mutex::new(Vec::new()));

        let sink1 = Arc::new(MockEventSink {
            events: Arc::clone(&list1),
        });
        let sink2 = Arc::new(MockEventSink {
            events: Arc::clone(&list2),
        });
        let composite = MultiSessionEventSink::new(vec![sink1, sink2]);

        let test_event = SessionEvent::SessionStarted {
            thread_id: Some(42),
            acp_session_id: "test-session-id".to_string(),
        };

        composite.publish(test_event).await;

        assert_eq!(list1.lock().await.len(), 1);
        assert_eq!(list2.lock().await.len(), 1);
    }

    #[tokio::test]
    async fn history_sink_limits_events() {
        let (sink, history) = HistorySessionEventSink::new(3);

        for i in 0..5 {
            sink.publish(SessionEvent::UserPrompt {
                thread_id: Some(1),
                acp_session_id: None,
                text: format!("msg {}", i),
                content: Vec::new(),
            })
            .await;
        }

        let h = history.lock().await;
        assert_eq!(h.len(), 3);
        if let SessionEvent::UserPrompt { text, .. } = &h[0] {
            assert_eq!(text, "msg 2");
        } else {
            panic!("wrong event type");
        }
        if let SessionEvent::UserPrompt { text, .. } = &h[2] {
            assert_eq!(text, "msg 4");
        } else {
            panic!("wrong event type");
        }
    }

    #[test]
    fn spawn_session_deserialization_with_agent_alias() {
        let json_with_agent = r#"{
            "type": "spawn_session",
            "project_path": "/Users/mats/some-project",
            "agent": "copilot",
            "thread_id": 42
        }"#;

        let cmd: WebSocketCommand = serde_json::from_str(json_with_agent).unwrap();
        if let WebSocketCommand::SpawnSession { agent_command, .. } = cmd {
            assert_eq!(agent_command, Some("copilot".to_string()));
        } else {
            panic!("wrong command variant");
        }

        let json_with_agent_command = r#"{
            "type": "spawn_session",
            "project_path": "/Users/mats/some-project",
            "agent_command": "copilot",
            "thread_id": 42
        }"#;

        let cmd2: WebSocketCommand = serde_json::from_str(json_with_agent_command).unwrap();
        if let WebSocketCommand::SpawnSession { agent_command, .. } = cmd2 {
            assert_eq!(agent_command, Some("copilot".to_string()));
        } else {
            panic!("wrong command variant");
        }
    }
}
