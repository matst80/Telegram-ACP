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
pub struct TerminalInfo {
    pub terminal_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thread_id: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub acp_session_id: Option<String>,
    pub cwd: std::path::PathBuf,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub command: Vec<String>,
    pub cols: u16,
    pub rows: u16,
    pub running: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DirectorySuggestion {
    pub path: String,
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
        #[serde(skip_serializing_if = "Option::is_none")]
        name: Option<String>,
        agent_name: Option<String>,
        agent_command: String,
        project_path: std::path::PathBuf,
        #[serde(default)]
        focus: bool,
    },
    SessionEnded {
        thread_id: Option<i32>,
        acp_session_id: Option<String>,
    },
    SessionRemoved {
        thread_id: i32,
        #[serde(skip_serializing_if = "Option::is_none")]
        acp_session_id: Option<String>,
    },
    PermissionRequest {
        thread_id: Option<i32>,
        acp_session_id: String,
        tool: String,
        args: serde_json::Value,
        request_id: String,
    },
    TelegramThreadBound {
        acp_session_id: String,
        thread_id: i32,
        name: String,
        created: bool,
    },
    Error {
        #[serde(skip_serializing_if = "Option::is_none")]
        in_reply_to: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        acp_session_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        thread_id: Option<i32>,
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
    TerminalCreated {
        terminal: TerminalInfo,
    },
    TerminalAttached {
        terminal_id: String,
        thread_id: Option<i32>,
        acp_session_id: Option<String>,
        cols: u16,
        rows: u16,
    },
    TerminalOutput {
        terminal_id: String,
        thread_id: Option<i32>,
        acp_session_id: Option<String>,
        sequence: u64,
        data: String,
    },
    TerminalSnapshot {
        terminal_id: String,
        thread_id: Option<i32>,
        acp_session_id: Option<String>,
        sequence: u64,
        cols: u16,
        rows: u16,
        cursor_row: u16,
        cursor_col: u16,
        data: String,
        running: bool,
    },
    TerminalResized {
        terminal_id: String,
        thread_id: Option<i32>,
        acp_session_id: Option<String>,
        cols: u16,
        rows: u16,
    },
    TerminalExited {
        terminal_id: String,
        thread_id: Option<i32>,
        acp_session_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        exit_code: Option<i32>,
    },
    TerminalClosed {
        terminal_id: String,
        thread_id: Option<i32>,
        acp_session_id: Option<String>,
    },
    TerminalError {
        #[serde(skip_serializing_if = "Option::is_none")]
        terminal_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        thread_id: Option<i32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        acp_session_id: Option<String>,
        message: String,
    },
    DirectorySuggestions {
        #[serde(skip_serializing_if = "Option::is_none")]
        thread_id: Option<i32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        acp_session_id: Option<String>,
        query: String,
        directories: Vec<DirectorySuggestion>,
    },
    Snapshot {
        sessions: Vec<crate::types::SessionInfo>,
        #[serde(skip_serializing_if = "Option::is_none")]
        rag_register_name: Option<String>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        projects: Vec<ProjectInfo>,
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        terminals: Vec<TerminalInfo>,
    },
    ClipboardUpdated {
        source: String,
        content: String,
        truncated: bool,
    },
    FindFilesResult {
        #[serde(skip_serializing_if = "Option::is_none")]
        thread_id: Option<i32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        acp_session_id: Option<String>,
        query: String,
        files: Vec<String>,
    },
    ReadFileResult {
        #[serde(skip_serializing_if = "Option::is_none")]
        thread_id: Option<i32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        acp_session_id: Option<String>,
        path: String,
        content: String,
        start_line: usize,
        line_count: usize,
        total_lines: usize,
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
    #[serde(alias = "spawn")]
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
    CreateTerminal {
        #[serde(default)]
        thread_id: Option<i32>,
        #[serde(default)]
        session_id: Option<String>,
        cols: u16,
        rows: u16,
        #[serde(default)]
        cwd: Option<String>,
        #[serde(default)]
        command: Option<Vec<String>>,
    },
    AttachTerminal {
        terminal_id: String,
        cols: u16,
        rows: u16,
    },
    TerminalInput {
        terminal_id: String,
        data: String,
    },
    TerminalResize {
        terminal_id: String,
        cols: u16,
        rows: u16,
    },
    CloseTerminal {
        terminal_id: String,
    },
    ListDirectories {
        #[serde(default)]
        thread_id: Option<i32>,
        #[serde(default)]
        session_id: Option<String>,
        query: String,
    },
    FindFiles {
        #[serde(default)]
        thread_id: Option<i32>,
        #[serde(default)]
        session_id: Option<String>,
        query: String,
        #[serde(default)]
        start_directory: Option<String>,
    },
    ReadFile {
        #[serde(default)]
        thread_id: Option<i32>,
        #[serde(default)]
        session_id: Option<String>,
        path: String,
        #[serde(default)]
        start_line: Option<usize>,
        #[serde(default)]
        line_count: Option<usize>,
    },
    ListTerminals,
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
    use crate::types::{SessionInfo, SessionStatus};
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
            name: Some("project: swift river".to_string()),
            agent_name: Some("copilot".to_string()),
            agent_command: "gemini --acp".to_string(),
            project_path: std::path::PathBuf::from("/tmp/project"),
            focus: false,
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

    #[test]
    fn clipboard_event_serializes_with_snake_case_tag() {
        let event = SessionEvent::ClipboardUpdated {
            source: "pbpaste".to_string(),
            content: "hello".to_string(),
            truncated: false,
        };

        let value = serde_json::to_value(event).unwrap();
        assert_eq!(value["type"], "clipboard_updated");
        assert_eq!(value["source"], "pbpaste");
        assert_eq!(value["content"], "hello");
        assert_eq!(value["truncated"], false);
    }
    
    #[test]
    fn create_terminal_deserialization() {
        let json = r#"{
            "type": "create_terminal",
            "thread_id": 7,
            "cols": 120,
            "rows": 40,
            "cwd": "subdir",
            "command": ["bash", "-lc", "printf hi"]
        }"#;

        let cmd: WebSocketCommand = serde_json::from_str(json).unwrap();
        match cmd {
            WebSocketCommand::CreateTerminal {
                thread_id,
                session_id,
                cols,
                rows,
                cwd,
                command,
            } => {
                assert_eq!(thread_id, Some(7));
                assert_eq!(session_id, None);
                assert_eq!(cols, 120);
                assert_eq!(rows, 40);
                assert_eq!(cwd.as_deref(), Some("subdir"));
                assert_eq!(
                    command,
                    Some(vec![
                        "bash".to_string(),
                        "-lc".to_string(),
                        "printf hi".to_string()
                    ])
                );
            }
            _ => panic!("wrong command variant"),
        }
    }

    #[test]
    fn list_directories_deserialization() {
        let json = r#"{
            "type": "list_directories",
            "thread_id": 7,
            "query": "~/github.com/ma"
        }"#;

        let cmd: WebSocketCommand = serde_json::from_str(json).unwrap();
        match cmd {
            WebSocketCommand::ListDirectories {
                thread_id,
                session_id,
                query,
            } => {
                assert_eq!(thread_id, Some(7));
                assert_eq!(session_id, None);
                assert_eq!(query, "~/github.com/ma");
            }
            _ => panic!("wrong command variant"),
        }
    }

    #[test]
    fn snapshot_serializes_terminals() {
        let event = SessionEvent::Snapshot {
            sessions: vec![SessionInfo {
                acp_session_id: "session-1".to_string(),
                project_path: std::path::PathBuf::from("/tmp/project"),
                status: SessionStatus::Idle,
                thread_id: Some(11),
                name: Some("demo".to_string()),
                agent_command: "copilot".to_string(),
                agent_name: Some("copilot".to_string()),
                available_commands: Vec::new(),
                history: Vec::new(),
            }],
            rag_register_name: Some("acp-mac".to_string()),
            projects: Vec::new(),
            terminals: vec![TerminalInfo {
                terminal_id: "term-1".to_string(),
                thread_id: Some(11),
                session_id: Some("session-1".to_string()),
                cwd: std::path::PathBuf::from("/tmp/project"),
                command: vec!["bash".to_string()],
                cols: 80,
                rows: 24,
                running: true,
                exit_code: None,
            }],
        };

        let json = serde_json::to_value(event).unwrap();
        assert_eq!(json["type"], "snapshot");
        assert_eq!(json["sessions"][0]["agent_name"], "copilot");
        assert_eq!(json["rag_register_name"], "acp-mac");
        assert_eq!(json["terminals"][0]["terminal_id"], "term-1");
        assert_eq!(json["terminals"][0]["cols"], 80);
    }

    #[test]
    fn directory_suggestions_serializes_with_snake_case_tag() {
        let event = SessionEvent::DirectorySuggestions {
            thread_id: Some(7),
            session_id: Some("session-1".to_string()),
            query: "~/github.com/ma".to_string(),
            directories: vec![DirectorySuggestion {
                path: "~/github.com/matst80/".to_string(),
            }],
        };

        let json = serde_json::to_value(event).unwrap();
        assert_eq!(json["type"], "directory_suggestions");
        assert_eq!(json["thread_id"], 7);
        assert_eq!(json["session_id"], "session-1");
        assert_eq!(json["query"], "~/github.com/ma");
        assert_eq!(json["directories"][0]["path"], "~/github.com/matst80/");
    }

    #[test]
    fn find_files_deserialization() {
        let json = r#"{
            "type": "find_files",
            "thread_id": 7,
            "query": "src/main"
        }"#;

        let cmd: WebSocketCommand = serde_json::from_str(json).unwrap();
        match cmd {
            WebSocketCommand::FindFiles {
                thread_id,
                session_id,
                query,
                start_directory,
            } => {
                assert_eq!(thread_id, Some(7));
                assert_eq!(session_id, None);
                assert_eq!(query, "src/main");
                assert_eq!(start_directory, None);
            }
            _ => panic!("wrong command variant"),
        }
    }

    #[test]
    fn find_files_result_serialization() {
        let event = SessionEvent::FindFilesResult {
            thread_id: Some(7),
            session_id: Some("session-1".to_string()),
            query: "src/main".to_string(),
            files: vec!["src/main.rs".to_string()],
        };

        let json = serde_json::to_value(event).unwrap();
        assert_eq!(json["type"], "find_files_result");
        assert_eq!(json["thread_id"], 7);
        assert_eq!(json["session_id"], "session-1");
        assert_eq!(json["query"], "src/main");
        assert_eq!(json["files"][0], "src/main.rs");
    }


    #[test]
    fn read_file_deserialization() {
        let json = r#"{
            "type": "read_file",
            "thread_id": 7,
            "path": "src/main.rs",
            "start_line": 1,
            "line_count": 400
        }"#;

        let cmd: WebSocketCommand = serde_json::from_str(json).unwrap();
        match cmd {
            WebSocketCommand::ReadFile {
                thread_id,
                session_id,
                path,
                start_line,
                line_count,
            } => {
                assert_eq!(thread_id, Some(7));
                assert_eq!(session_id, None);
                assert_eq!(path, "src/main.rs");
                assert_eq!(start_line, Some(1));
                assert_eq!(line_count, Some(400));
            }
            _ => panic!("wrong command variant"),
        }
    }

    #[test]
    fn read_file_result_serialization() {
        let event = SessionEvent::ReadFileResult {
            thread_id: Some(7),
            session_id: Some("session-1".to_string()),
            path: "src/main.rs".to_string(),
            content: "fn main() {}".to_string(),
            start_line: 1,
            line_count: 400,
            total_lines: 1,
        };

        let json = serde_json::to_value(event).unwrap();
        assert_eq!(json["type"], "read_file_result");
        assert_eq!(json["thread_id"], 7);
        assert_eq!(json["session_id"], "session-1");
        assert_eq!(json["path"], "src/main.rs");
        assert_eq!(json["content"], "fn main() {}");
        assert_eq!(json["start_line"], 1);
        assert_eq!(json["line_count"], 400);
        assert_eq!(json["total_lines"], 1);
    }
}


