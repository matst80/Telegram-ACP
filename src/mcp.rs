use anyhow::{anyhow, Result};
use futures::channel::mpsc;
use futures::StreamExt;
use rmcp::handler::server::router::tool::ToolRouter;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{
    CallToolResult, Content, ErrorCode, ErrorData, Implementation, ProtocolVersion,
    ServerCapabilities, ServerInfo,
};
use rmcp::service::{RxJsonRpcMessage, TxJsonRpcMessage};
use rmcp::{tool, tool_handler, tool_router, RoleServer, ServerHandler, ServiceExt};
use schemars::JsonSchema;
use serde::Deserialize;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use teloxide::prelude::*;
use teloxide::types::{InputFile, MessageId, ThreadId};
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::ipc;
use crate::telegraph;
use crate::types::{DaemonCommand, DaemonResponse};

#[derive(Clone)]
struct McpServer {
    bot: Option<Bot>,
    telegraph: Arc<telegraph_rs::Telegraph>,
    chat_id: Option<ChatId>,
    thread_id: Option<i32>,
    project_path: PathBuf,
    socket_path: PathBuf,
    tool_router: ToolRouter<McpServer>,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct SpawnAgentArgs {
    /// Absolute or project-relative path to the project directory
    directory: String,
    /// Optional agent name (defined in config)
    agent: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct UploadMarkdownArgs {
    /// Absolute or project-relative path to a Markdown file
    path: String,
    title: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct UploadFileArgs {
    /// Absolute or project-relative path
    path: String,
    caption: Option<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct RenameTopicArgs {
    /// The new name for the Telegram topic
    name: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct UploadImageArgs {
    /// Absolute or project-relative path
    path: String,
    caption: Option<String>,
}

#[tool_router]
impl McpServer {
    /// Change the name of the current Telegram forum topic (thread)
    #[tool]
    async fn rename_topic(
        &self,
        Parameters(args): Parameters<RenameTopicArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let (Some(bot), Some(chat_id), Some(tid)) = (&self.bot, self.chat_id, self.thread_id) else {
            return Err(ErrorData::new(
                ErrorCode::INVALID_PARAMS,
                "No Telegram bot/chat/thread associated with this session",
                None,
            ));
        };
        let thread_id = ThreadId(MessageId(tid));
        if let Err(e) = bot
            .edit_forum_topic(chat_id, thread_id)
            .name(&args.name)
            .await
        {
            return Err(ErrorData::new(
                ErrorCode::INTERNAL_ERROR,
                format!("Failed to rename topic: {e}"),
                None,
            ));
        }

        Ok(CallToolResult::success(vec![Content::text(format!(
            "Topic successfully renamed to: {}",
            args.name
        ))]))
    }

    /// Create a new agent session in a new Telegram topic
    #[tool]
    async fn spawn_agent(
        &self,
        Parameters(args): Parameters<SpawnAgentArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let resolved_path = resolve_path(&self.project_path, &args.directory);

        let cmd = DaemonCommand::NewSession {
            path: resolved_path,
            prompt: None,
            agent: args.agent,
        };

        match ipc::send_command(&self.socket_path, &cmd).await {
            Ok(DaemonResponse::SessionCreated {
                acp_session_id: _,
                topic_url,
            }) => Ok(CallToolResult::success(vec![Content::text(format!(
                "New agent session spawned: {topic_url}"
            ))])),
            Ok(DaemonResponse::Error { message }) => Err(ErrorData::new(
                ErrorCode::INTERNAL_ERROR,
                format!("Daemon failed to spawn session: {message}"),
                None,
            )),
            Ok(_) => Err(ErrorData::new(
                ErrorCode::INTERNAL_ERROR,
                "Unexpected daemon response",
                None,
            )),
            Err(e) => Err(ErrorData::new(
                ErrorCode::INTERNAL_ERROR,
                format!("Failed to communicate with daemon: {e}"),
                None,
            )),
        }
    }

    /// Render Markdown file to Telegraph and send link to the user
    #[tool]
    async fn upload_markdown(
        &self,
        Parameters(args): Parameters<UploadMarkdownArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let resolved = resolve_path(&self.project_path, &args.path);
        let markdown = tokio::fs::read_to_string(&resolved)
            .await
            .map_err(|e| ErrorData::new(ErrorCode::INVALID_PARAMS, e.to_string(), None))?;
        let title = args.title.unwrap_or_else(|| "Markdown Upload".to_string());
        let url = telegraph::create_markdown_post(&self.telegraph, &title, &markdown)
            .await
            .map_err(|e| ErrorData::new(ErrorCode::INTERNAL_ERROR, e.to_string(), None))?;

        let (Some(bot), Some(chat_id), Some(tid)) = (&self.bot, self.chat_id, self.thread_id) else {
            return Ok(CallToolResult::success(vec![Content::text(url)]));
        };
        let thread_id = ThreadId(MessageId(tid));
        if let Err(e) = bot
            .send_message(chat_id, format!("Telegraph: {url}"))
            .message_thread_id(thread_id)
            .await
        {
            return Err(ErrorData::new(
                ErrorCode::INTERNAL_ERROR,
                format!("Failed to send Telegram message: {e}"),
                None,
            ));
        }

        Ok(CallToolResult::success(vec![Content::text(url)]))
    }

    /// Upload a file to Telegram
    #[tool]
    async fn upload_file(
        &self,
        Parameters(args): Parameters<UploadFileArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let (input, filename) = build_input_file(&self.project_path, &args.path)?;

        let (Some(bot), Some(chat_id), Some(tid)) = (&self.bot, self.chat_id, self.thread_id) else {
            return Err(ErrorData::new(
                ErrorCode::INVALID_PARAMS,
                "No Telegram thread associated with this session",
                None,
            ));
        };
        let thread_id = ThreadId(MessageId(tid));
        let mut request = bot
            .send_document(chat_id, input)
            .message_thread_id(thread_id);
        if let Some(caption) = args.caption {
            request = request.caption(caption);
        }
        request
            .await
            .map_err(|e| ErrorData::new(ErrorCode::INTERNAL_ERROR, e.to_string(), None))?;

        Ok(CallToolResult::success(vec![Content::text(format!(
            "Uploaded file: {filename}"
        ))]))
    }

    /// Upload an image to Telegram
    #[tool]
    async fn upload_image(
        &self,
        Parameters(args): Parameters<UploadImageArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let (input, filename) = build_input_file(&self.project_path, &args.path)?;

        let (Some(bot), Some(chat_id), Some(tid)) = (&self.bot, self.chat_id, self.thread_id) else {
            return Err(ErrorData::new(
                ErrorCode::INVALID_PARAMS,
                "No Telegram thread associated with this session",
                None,
            ));
        };
        let thread_id = ThreadId(MessageId(tid));
        let mut request = bot
            .send_photo(chat_id, input)
            .message_thread_id(thread_id);
        if let Some(caption) = args.caption {
            request = request.caption(caption);
        }
        request
            .await
            .map_err(|e| ErrorData::new(ErrorCode::INTERNAL_ERROR, e.to_string(), None))?;

        Ok(CallToolResult::success(vec![Content::text(format!(
            "Uploaded image: {filename}"
        ))]))
    }
}

#[tool_handler]
impl ServerHandler for McpServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(
                Implementation::new("telegram-acp", env!("CARGO_PKG_VERSION"))
                    .with_title("Telegram MCP Relay"),
            )
            .with_protocol_version(ProtocolVersion::V_2025_03_26)
    }
}

impl McpServer {
    fn new(
        bot: Option<Bot>,
        telegraph: Arc<telegraph_rs::Telegraph>,
        chat_id: Option<ChatId>,
        thread_id: Option<i32>,
        project_path: PathBuf,
        socket_path: PathBuf,
    ) -> Self {
        Self {
            bot,
            telegraph,
            chat_id,
            thread_id,
            project_path,
            socket_path,
            tool_router: Self::tool_router(),
        }
    }
}

pub struct McpSession {
    pub id: String,
    incoming_tx: Mutex<mpsc::UnboundedSender<RxJsonRpcMessage<RoleServer>>>,
    outgoing_rx: Mutex<mpsc::UnboundedReceiver<TxJsonRpcMessage<RoleServer>>>,
}

impl McpSession {
    pub async fn new(
        bot: Option<Bot>,
        telegraph: Arc<telegraph_rs::Telegraph>,
        chat_id: Option<ChatId>,
        thread_id: Option<i32>,
        project_path: PathBuf,
        socket_path: PathBuf,
    ) -> Result<Self> {
        let id = Uuid::new_v4().to_string();
        let (incoming_tx, incoming_rx) = mpsc::unbounded();
        let (outgoing_tx, outgoing_rx) = mpsc::unbounded();

        let server = McpServer::new(
            bot,
            telegraph,
            chat_id,
            thread_id,
            project_path,
            socket_path,
        );
        let session_id_for_log = id.clone();
        tokio::task::spawn_local(async move {
            tracing::debug!(session_id = %session_id_for_log, "MCP server task started, waiting for initialize");
            match server.serve((outgoing_tx, incoming_rx)).await {
                Ok(service) => {
                    tracing::debug!(session_id = %session_id_for_log, "MCP server handshake complete, waiting for session end");
                    let _ = service.waiting().await;
                    tracing::debug!(session_id = %session_id_for_log, "MCP server session ended");
                }
                Err(e) => {
                    tracing::warn!(session_id = %session_id_for_log, "MCP server init error: {e}")
                }
            }
        });

        Ok(Self {
            id,
            incoming_tx: Mutex::new(incoming_tx),
            outgoing_rx: Mutex::new(outgoing_rx),
        })
    }

    pub async fn send(&self, message: RxJsonRpcMessage<RoleServer>) -> Result<()> {
        tracing::debug!(session_id = %self.id, "MCP session: sending message to server");
        let tx = self.incoming_tx.lock().await;
        tx.unbounded_send(message).map_err(|e| {
            tracing::error!(session_id = %self.id, "MCP incoming channel closed: {e}");
            anyhow!("MCP incoming channel closed")
        })?;
        tracing::debug!(session_id = %self.id, "MCP session: message enqueued");
        Ok(())
    }

pub async fn next_response(&self) -> Option<TxJsonRpcMessage<RoleServer>> {
        tracing::debug!(session_id = %self.id, "MCP session: waiting for response");
        let mut rx = self.outgoing_rx.lock().await;
        let result = rx.next().await;
        match &result {
            Some(_) => tracing::debug!(session_id = %self.id, "MCP session: got response"),
            None => {
                tracing::warn!(session_id = %self.id, "MCP session: outgoing channel closed, no response")
            }
        }
        result
    }
}

pub fn build_mcp_servers(
    mcp_session_id: &str,
    socket_path: &std::path::Path,
    config: &crate::config::Config,
) -> Result<Vec<agent_client_protocol::McpServer>> {
    let exe_path = std::env::current_exe()
        .map_err(|e| anyhow!("Failed to resolve current executable: {e}"))?;
    let args = vec![
        "mcp-relay".to_string(),
        "--session".to_string(),
        mcp_session_id.to_string(),
        "--socket".to_string(),
        socket_path.to_string_lossy().to_string(),
    ];
    let mut servers = vec![agent_client_protocol::McpServer::Stdio(
        agent_client_protocol::McpServerStdio::new("telegram-acp-relay", exe_path).args(args),
    )];

    for (name, mcp_cfg) in &config.mcp_servers {
        let ty = mcp_cfg.r#type.as_deref().unwrap_or("").to_lowercase();
        if ty == "stdio" || mcp_cfg.command.is_some() {
            if let Some(cmd) = &mcp_cfg.command {
                let mut s = agent_client_protocol::McpServerStdio::new(name, cmd);
                if let Some(args) = &mcp_cfg.args {
                    s = s.args(args.clone());
                }
                servers.push(agent_client_protocol::McpServer::Stdio(s));
            }
        } else {
            let url_val = mcp_cfg.url.clone()
                .or_else(|| mcp_cfg.server_url_camel.clone())
                .or_else(|| mcp_cfg.server_url_snake.clone());
            if let Some(u) = url_val {
                if ty == "sse" {
                    servers.push(agent_client_protocol::McpServer::Sse(agent_client_protocol::McpServerSse::new(name, u)));
                } else {
                    servers.push(agent_client_protocol::McpServer::Http(agent_client_protocol::McpServerHttp::new(name, u)));
                }
            }
        }
    }

    Ok(servers)
}

pub fn mcp_expects_response(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::Object(map) => map.get("id").map(|id| !id.is_null()).unwrap_or(false),
        serde_json::Value::Array(items) => items.iter().any(|item| {
            item.as_object()
                .and_then(|map| map.get("id"))
                .map(|id| !id.is_null())
                .unwrap_or(false)
        }),
        _ => false,
    }
}

fn build_input_file(project_path: &Path, path: &str) -> Result<(InputFile, String), ErrorData> {
    let resolved = resolve_path(project_path, path);
    let display = resolved
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("upload.bin")
        .to_string();
    Ok((InputFile::file(resolved), display))
}

fn resolve_path(project_path: &Path, path: &str) -> PathBuf {
    let candidate = PathBuf::from(path);
    if candidate.is_absolute() {
        candidate
    } else {
        project_path.join(candidate)
    }
}
