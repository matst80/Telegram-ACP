use std::sync::Arc;

use async_trait::async_trait;
use serde::Serialize;
use tokio::sync::{broadcast, mpsc, Mutex};

use crate::types::AgentEvent;

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SessionEvent {
    UserPrompt {
        thread_id: i32,
        acp_session_id: Option<String>,
        text: String,
    },
    AgentUpdate {
        thread_id: i32,
        acp_session_id: String,
        event: AgentEvent,
    },
    SessionStarted {
        thread_id: i32,
        acp_session_id: String,
    },
    SessionSwitched {
        thread_id: i32,
        acp_session_id: String,
    },
    SessionEnded {
        thread_id: i32,
        acp_session_id: Option<String>,
    },
}

#[async_trait]
pub trait SessionEventSink: Send + Sync {
    async fn publish(&self, event: SessionEvent);
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

pub struct ChannelSessionEventSink {
    tx: mpsc::UnboundedSender<SessionEvent>,
}

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
            thread_id: 42,
            acp_session_id: "test-session-id".to_string(),
        };

        composite.publish(test_event).await;

        assert_eq!(list1.lock().await.len(), 1);
        assert_eq!(list2.lock().await.len(), 1);
    }
}
