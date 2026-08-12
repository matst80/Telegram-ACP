use anyhow::{Context, Result};
use serde::Deserialize;
use std::collections::HashMap;
use std::path::PathBuf;

#[derive(Debug, Clone, Deserialize, Default)]
pub struct FileMcpServerConfig {
    #[serde(rename = "type")]
    pub r#type: Option<String>,
    pub command: Option<String>,
    pub args: Option<Vec<String>>,
    pub url: Option<String>,
    #[serde(rename = "serverUrl")]
    pub server_url_camel: Option<String>,
    #[serde(rename = "server_url")]
    pub server_url_snake: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub bot_token: Option<String>,
    pub chat_id: Option<i64>,
    pub telegraph_author: Option<String>,
    #[allow(dead_code)]
    pub telegraph_author_url: Option<String>,
    pub socket_path: PathBuf,
    pub websocket_bind: Option<String>,
    pub default_agent: Option<String>,
    pub agents: HashMap<String, String>,
    pub websocket_history_limit: usize,
    pub websocket_clipboard: bool,
    pub global_clipboard_intercept: bool,
    pub websocket_clipboard_poll_ms: u64,
    pub websocket_clipboard_max_bytes: usize,
    pub mcp_servers: HashMap<String, FileMcpServerConfig>,
    pub rag_register_url: Option<String>,
    pub rag_token: Option<String>,
    pub rag_register_name: Option<String>,
    pub rag_register_host: Option<String>,
    pub project_root: Option<PathBuf>,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct FileConfig {
    bot_token: Option<String>,
    chat_id: Option<i64>,
    telegraph_author: Option<String>,
    telegraph_author_url: Option<String>,
    socket_path: Option<PathBuf>,
    websocket_bind: Option<String>,
    websocket_history_limit: Option<usize>,
    websocket_clipboard: Option<bool>,
    global_clipboard_intercept: Option<bool>,
    websocket_clipboard_poll_ms: Option<u64>,
    websocket_clipboard_max_bytes: Option<usize>,
    default_agent: Option<String>,
    #[serde(alias = "mcpServers")]
    mcp_servers: Option<HashMap<String, FileMcpServerConfig>>,
    rag_register_url: Option<String>,
    rag_token: Option<String>,
    rag_register_name: Option<String>,
    rag_register_host: Option<String>,
    pub project_root: Option<PathBuf>,
    #[serde(flatten)]
    extra_tables: HashMap<String, toml::Table>,
}

impl Config {
    pub fn load() -> Result<Self> {
        let config_path = dirs_config_path();
        let file_config = if config_path.exists() {
            let contents = std::fs::read_to_string(&config_path).with_context(|| {
                format!("Failed to read config file: {}", config_path.display())
            })?;
            toml::from_str::<FileConfig>(&contents).with_context(|| {
                format!("Failed to parse config file: {}", config_path.display())
            })?
        } else {
            FileConfig::default()
        };

        let bot_token = env_or("TELEGRAM_ACP_BOT_TOKEN", file_config.bot_token);

        let chat_id = env_or(
            "TELEGRAM_ACP_CHAT_ID",
            file_config.chat_id.map(|id| id.to_string()),
        )
        .and_then(|id| id.parse::<i64>().ok());

        let socket_path = env_or(
            "TELEGRAM_ACP_SOCKET_PATH",
            file_config
                .socket_path
                .map(|p| p.to_string_lossy().into_owned()),
        )
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/tmp/telegram-acp.sock"));
        let websocket_bind = env_or("TELEGRAM_ACP_WEBSOCKET_BIND", file_config.websocket_bind);
        let websocket_history_limit = env_or(
            "TELEGRAM_ACP_WEBSOCKET_HISTORY_LIMIT",
            file_config.websocket_history_limit.map(|l| l.to_string()),
        )
        .and_then(|l| l.parse().ok())
        .unwrap_or(20);
        let websocket_clipboard = env_or(
            "TELEGRAM_ACP_WEBSOCKET_CLIPBOARD",
            file_config.websocket_clipboard.map(|value| value.to_string()),
        )
        .map(|value| matches!(value.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
        .unwrap_or(true);
        let global_clipboard_intercept = env_or(
            "TELEGRAM_ACP_GLOBAL_CLIPBOARD_INTERCEPT",
            file_config
                .global_clipboard_intercept
                .map(|value| value.to_string()),
        )
        .map(|value| matches!(value.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
        .unwrap_or(default_global_clipboard_intercept());
        let websocket_clipboard_poll_ms = env_or(
            "TELEGRAM_ACP_WEBSOCKET_CLIPBOARD_POLL_MS",
            file_config.websocket_clipboard_poll_ms.map(|value| value.to_string()),
        )
        .and_then(|value| value.parse().ok())
        .unwrap_or(750);
        let websocket_clipboard_max_bytes = env_or(
            "TELEGRAM_ACP_WEBSOCKET_CLIPBOARD_MAX_BYTES",
            file_config.websocket_clipboard_max_bytes.map(|value| value.to_string()),
        )
        .and_then(|value| value.parse().ok())
        .unwrap_or(4096);

        let agents = parse_agents(&file_config.extra_tables);
        let default_agent = env_or("TELEGRAM_ACP_DEFAULT_AGENT", file_config.default_agent)
            .or_else(|| {
                if agents.len() == 1 {
                    agents.keys().next().cloned()
                } else {
                    None
                }
            });
        if let Some(agent) = &default_agent {
            ensure_agent_exists(agent, &agents)?;
        }

        let telegraph_author = env_or(
            "TELEGRAM_ACP_TELEGRAPH_AUTHOR",
            file_config.telegraph_author,
        );
        let telegraph_author_url = env_or(
            "TELEGRAM_ACP_TELEGRAPH_AUTHOR_URL",
            file_config.telegraph_author_url,
        );

        let mcp_servers = file_config.mcp_servers.unwrap_or_default();

        let rag_register_url = env_or("TELEGRAM_ACP_RAG_REGISTER_URL", file_config.rag_register_url);
        let rag_token = env_or("TELEGRAM_ACP_RAG_TOKEN", file_config.rag_token);
        let rag_register_name = env_or(
            "TELEGRAM_ACP_RAG_REGISTER_NAME",
            file_config.rag_register_name,
        )
        .or_else(current_hostname);
        let rag_register_host = env_or("TELEGRAM_ACP_RAG_REGISTER_HOST", file_config.rag_register_host);
        let project_root = env_or(
            "TELEGRAM_ACP_PROJECT_ROOT",
            file_config
                .project_root
                .map(|p| p.to_string_lossy().into_owned()),
        )
        .map(PathBuf::from);

        Ok(Config {
            bot_token,
            chat_id,
            telegraph_author,
            telegraph_author_url,
            socket_path,
            websocket_bind,
            default_agent,
            agents,
            websocket_history_limit,
            websocket_clipboard,
            global_clipboard_intercept,
            websocket_clipboard_poll_ms,
            websocket_clipboard_max_bytes,
            mcp_servers,
            rag_register_url,
            rag_token,
            rag_register_name,
            rag_register_host,
            project_root,
        })
    }

    #[allow(dead_code)]
    pub fn resolve_agent_command(&self, selected_agent: Option<&str>) -> Result<String> {
        self.resolve_agent(selected_agent)
            .map(|(_, command)| command)
    }

    pub fn resolve_agent(&self, selected_agent: Option<&str>) -> Result<(String, String)> {
        let selected_agent = match selected_agent.map(str::trim).filter(|v| !v.is_empty()) {
            Some(agent) => agent,
            None => self
                .default_agent
                .as_deref()
                .ok_or_else(|| anyhow::anyhow!("No default agent configured"))?,
        };

        let command =
            self.agents.get(selected_agent).cloned().ok_or_else(|| {
                anyhow::anyhow!(unknown_agent_message(selected_agent, &self.agents))
            })?;

        Ok((selected_agent.to_string(), command))
    }
}

fn dirs_config_path() -> PathBuf {
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
    PathBuf::from(home)
        .join(".config")
        .join("telegram-acp")
        .join("config.toml")
}

fn env_or(key: &str, fallback: Option<String>) -> Option<String> {
    std::env::var(key).ok().or(fallback)
}

#[cfg(target_os = "macos")]
fn default_global_clipboard_intercept() -> bool {
    false
}

#[cfg(not(target_os = "macos"))]
fn default_global_clipboard_intercept() -> bool {
    true
}

fn current_hostname() -> Option<String> {
    if let Ok(hostname) = std::env::var("HOSTNAME") {
        let trimmed = hostname.trim();
        if !trimmed.is_empty() {
            return Some(trimmed.to_string());
        }
    }

    let output = std::process::Command::new("hostname").output().ok()?;
    if !output.status.success() {
        return None;
    }

    let hostname = String::from_utf8_lossy(&output.stdout);
    let trimmed = hostname.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

fn parse_agents(extra_tables: &HashMap<String, toml::Table>) -> HashMap<String, String> {
    let mut agents = HashMap::new();
    for (name, table) in extra_tables {
        if let Some(toml::Value::String(cmd)) = table.get("cmd") {
            let trimmed = cmd.trim();
            if !trimmed.is_empty() {
                agents.insert(name.clone(), trimmed.to_string());
            }
        }
    }
    agents
}

fn ensure_agent_exists(default_agent: &str, agents: &HashMap<String, String>) -> Result<()> {
    if agents.contains_key(default_agent) {
        return Ok(());
    }
    anyhow::bail!(
        "default_agent '{}' has no matching [<agent>] table with cmd. {}",
        default_agent,
        unknown_agent_message(default_agent, agents)
    );
}

fn unknown_agent_message(agent: &str, agents: &HashMap<String, String>) -> String {
    let mut available: Vec<String> = agents.keys().cloned().collect();
    available.sort();
    if available.is_empty() {
        format!(
            "Unknown agent '{}'. No agents are configured. Add tables like [codex] with cmd = \"...\".",
            agent
        )
    } else {
        format!(
            "Unknown agent '{}'. Available agents: {}",
            agent,
            available.join(", ")
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_mcp_servers() {
        let toml_content = r#"
            bot_token = "test"
            chat_id = 123
            default_agent = "claude"

            [claude]
            cmd = "claude-agent-acp"

            [mcp_servers.rag]
            serverUrl = "https://rag.k6n.net/mcp"

            [mcp_servers.sqlite]
            command = "sqlite_mcp"
            args = ["--db", "test.db"]

            [mcp_servers.mysse]
            type = "sse"
            url = "https://example.com/sse"
        "#;

        let file_cfg: FileConfig = toml::from_str(toml_content).unwrap();
        let mcp_servers = file_cfg.mcp_servers.unwrap();

        assert_eq!(mcp_servers.len(), 3);

        let rag = mcp_servers.get("rag").unwrap();
        assert_eq!(rag.server_url_camel.as_deref(), Some("https://rag.k6n.net/mcp"));

        let sqlite = mcp_servers.get("sqlite").unwrap();
        assert_eq!(sqlite.command.as_deref(), Some("sqlite_mcp"));
        assert_eq!(sqlite.args.as_ref().unwrap(), &vec!["--db".to_string(), "test.db".to_string()]);

        let mysse = mcp_servers.get("mysse").unwrap();
        assert_eq!(mysse.r#type.as_deref(), Some("sse"));
        assert_eq!(mysse.url.as_deref(), Some("https://example.com/sse"));
    }

    #[test]
    fn current_hostname_prefers_hostname_env() {
        std::env::set_var("HOSTNAME", "host-from-env");

        let hostname = current_hostname();

        std::env::remove_var("HOSTNAME");
        assert_eq!(hostname.as_deref(), Some("host-from-env"));
    }

    #[test]
    fn global_clipboard_intercept_env_parses_true() {
        std::env::set_var("TELEGRAM_ACP_GLOBAL_CLIPBOARD_INTERCEPT", "true");

        let enabled = env_or("TELEGRAM_ACP_GLOBAL_CLIPBOARD_INTERCEPT", None)
            .map(|value| matches!(value.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
            .unwrap_or(default_global_clipboard_intercept());

        std::env::remove_var("TELEGRAM_ACP_GLOBAL_CLIPBOARD_INTERCEPT");
        assert!(enabled);
    }
}
