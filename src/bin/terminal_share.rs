use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

use telegram_acp::config::Config;
use telegram_acp::daemon::DaemonHandle;
use telegram_acp::relay::{
    BroadcastSessionEventSink, MultiSessionEventSink, SessionEventSink, SessionStateProvider,
};
use telegram_acp::session_manager::SessionManager;
use telegram_acp::terminal::TerminalManager;

#[derive(Parser)]
#[command(name = "terminal-share", about = "Minimal PTY Terminal & Workspace Share Server")]
struct Cli {
    /// WebSocket bind address (e.g. 0.0.0.0:8000)
    #[arg(long, default_value = "0.0.0.0:8000", env = "TERMINAL_SHARE_BIND")]
    bind: String,

    /// Project root for directory listing and searching
    #[arg(long, env = "TERMINAL_SHARE_PROJECT_ROOT")]
    project_root: Option<PathBuf>,

    /// RAG registration URL
    #[arg(long, env = "TERMINAL_SHARE_RAG_REGISTER_URL")]
    rag_register_url: Option<String>,

    /// RAG token
    #[arg(long, env = "TERMINAL_SHARE_RAG_TOKEN")]
    rag_token: Option<String>,

    /// RAG registration name
    #[arg(long, env = "TERMINAL_SHARE_RAG_REGISTER_NAME")]
    rag_register_name: Option<String>,

    /// RAG registration host
    #[arg(long, env = "TERMINAL_SHARE_RAG_REGISTER_HOST")]
    rag_register_host: Option<String>,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let local = tokio::task::LocalSet::new();
    local.run_until(async_main()).await
}

async fn async_main() -> Result<()> {
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));

    tracing_subscriber::registry()
        .with(env_filter)
        .with(tracing_subscriber::fmt::layer())
        .init();

    let cli = Cli::parse();

    tracing::info!(bind = %cli.bind, "Starting minimal terminal-share server");

    let mut config = Config::load().unwrap_or_else(|_| Config {
        bot_token: None,
        chat_id: None,
        telegraph_author: None,
        telegraph_author_url: None,
        socket_path: PathBuf::from("/tmp/terminal-share.sock"),
        websocket_bind: Some(cli.bind.clone()),
        default_agent: None,
        agents: std::collections::HashMap::new(),
        websocket_history_limit: 20,
        websocket_clipboard: true,
        global_clipboard_intercept: false,
        websocket_clipboard_poll_ms: 750,
        websocket_clipboard_max_bytes: 4096,
        mcp_servers: std::collections::HashMap::new(),
        rag_register_url: None,
        rag_token: None,
        rag_register_name: None,
        rag_register_host: None,
        project_root: None,
    });
    config.socket_path = PathBuf::from("/tmp/terminal-share.sock");
    config.websocket_bind = Some(cli.bind.clone());
    if cli.rag_register_url.is_some() {
        config.rag_register_url = cli.rag_register_url;
    }
    if cli.rag_token.is_some() {
        config.rag_token = cli.rag_token;
    }
    if cli.rag_register_name.is_some() {
        config.rag_register_name = cli.rag_register_name;
    }
    if cli.rag_register_host.is_some() {
        config.rag_register_host = cli.rag_register_host;
    }
    if cli.project_root.is_some() {
        config.project_root = cli.project_root;
    }

    let (local_start_tx, mut local_start_rx) =
        mpsc::unbounded_channel::<telegram_acp::daemon::StartSessionRequest>();
    let websocket_events = Arc::new(BroadcastSessionEventSink::new(256));
    let session_event_sink: Arc<dyn SessionEventSink> = Arc::new(MultiSessionEventSink::new(vec![
        websocket_events.clone() as Arc<dyn SessionEventSink>,
    ]));

    let telegraph = Arc::new(telegram_acp::telegraph::create_account(Some("terminal-share")).await?);

    let daemon = Arc::new(DaemonHandle {
        config: config.clone(),
        bot: None,
        telegraph,
        session_event_sink: session_event_sink.clone(),
        start_time: std::sync::atomic::AtomicI64::new(chrono::Utc::now().timestamp()),
        local_start_tx,
        session_manager: SessionManager::new(),
        terminal_manager: Arc::new(TerminalManager::new(
            websocket_events.clone() as Arc<dyn SessionEventSink>
        )),
        pending_permissions: Arc::new(dashmap::DashMap::new()),
        mdns: std::sync::Mutex::new(None),
    });

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

    if let Ok(addr) = cli.bind.parse::<std::net::SocketAddr>() {
        tracing::info!(bind_addr = %cli.bind, port = addr.port(), "Starting mDNS advertising");
        match telegram_acp::websocket::advertise_service(addr.port()) {
            Ok(mdns_daemon) => {
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
                telegram_acp::daemon::rag::run_rag_registration(config, port).await;
            });
        }
    }

    let state_provider = daemon.clone() as Arc<dyn SessionStateProvider>;
    let command_handler = daemon.clone() as Arc<dyn telegram_acp::relay::WebSocketCommandHandler>;

    let bind_addr = cli.bind.clone();
    tokio::task::spawn_local(async move {
        if let Err(err) = telegram_acp::websocket::run_server(
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

    tracing::info!("Terminal share server listening on ws://{}", cli.bind);
    tokio::signal::ctrl_c().await?;
    tracing::info!("Shutting down terminal share server");

    Ok(())
}
