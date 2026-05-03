use std::sync::Arc;
use std::collections::HashMap;
use mdns_sd::{ServiceDaemon, ServiceInfo};

use anyhow::Result;
use futures::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;
use tokio_tungstenite::tungstenite::Message;
use crate::relay::{BroadcastSessionEventSink, SessionStateProvider, WebSocketCommand, WebSocketCommandHandler};

pub async fn run_server(
    bind_addr: &str,
    events: Arc<BroadcastSessionEventSink>,
    state_provider: Arc<dyn SessionStateProvider>,
    command_handler: Arc<dyn WebSocketCommandHandler>,
) -> Result<()> {
    let listener = TcpListener::bind(bind_addr).await?;
    tracing::info!(bind_addr, "Websocket listener started");

    loop {
        let (stream, peer_addr) = listener.accept().await?;
        let events = events.clone();
        let state_provider = state_provider.clone();
        let command_handler = command_handler.clone();
        tokio::spawn(async move {
            if let Err(err) =
                handle_connection(stream, events.subscribe(), state_provider, command_handler).await
            {
                tracing::warn!(peer = %peer_addr, "Websocket listener closed: {err}");
            }
        });
    }
}

async fn handle_connection(
    stream: TcpStream,
    mut events: broadcast::Receiver<String>,
    state_provider: Arc<dyn SessionStateProvider>,
    command_handler: Arc<dyn WebSocketCommandHandler>,
) -> Result<()> {
    let token = std::env::var("ACP_WS_TOKEN")
        .or_else(|_| std::env::var("TELEGRAM_ACP_WS_TOKEN"))
        .ok();
    let mut auth_error = false;
    let websocket = tokio_tungstenite::accept_hdr_async(stream, |req: &tokio_tungstenite::tungstenite::handshake::server::Request, mut res: tokio_tungstenite::tungstenite::handshake::server::Response| {
        if let Some(t) = &token {
            let mut authorized = false;
            if let Some(auth) = req.headers().get("Authorization") {
                if let Ok(auth_str) = auth.to_str() {
                    if auth_str.trim_start_matches("Bearer ") == t {
                        authorized = true;
                    }
                }
            } else if let Some(query) = req.uri().query() {
                for param in query.split('&') {
                    if let Some((k, v)) = param.split_once('=') {
                        if k == "token" && v == t {
                            authorized = true;
                        }
                    }
                }
            }
            if !authorized {
                auth_error = true;
                *res.status_mut() = tokio_tungstenite::tungstenite::http::StatusCode::UNAUTHORIZED;
            }
        }
        Ok::<_, tokio_tungstenite::tungstenite::http::Response<Option<String>>>(res)
    }).await;

    if auth_error {
        return Err(anyhow::anyhow!("Unauthorized"));
    }
    let websocket = websocket?;

    let (mut writer, mut reader) = websocket.split();

    // Send initial snapshot
    for event in state_provider.get_snapshot().await {
        let payload = serde_json::to_string(&event)?;
        writer.send(Message::Text(payload)).await?;
    }

    loop {
        tokio::select! {
            event = events.recv() => match event {
                Ok(payload) => {
                    writer.send(Message::Text(payload)).await?;
                }
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    tracing::warn!(skipped, "Websocket listener lagged behind session events");
                }
                Err(broadcast::error::RecvError::Closed) => break,
            },
            incoming = reader.next() => match incoming {
                Some(Ok(Message::Close(_))) | None => break,
                Some(Ok(Message::Ping(payload))) => {
                    writer.send(Message::Pong(payload)).await?;
                }
                Some(Ok(Message::Text(text))) => {
                    match serde_json::from_str::<WebSocketCommand>(&text) {
                        Ok(cmd) => {
                            if let Err(err) = command_handler.handle_command(cmd).await {
                                tracing::warn!("Failed to handle websocket command: {err}");
                                // Optionally send error back to client
                            }
                        }
                        Err(err) => {
                            tracing::warn!("Failed to parse websocket command: {err}");
                        }
                    }
                }
                Some(Ok(_)) => {}
                Some(Err(err)) => return Err(err.into()),
            },
        }
    }

    Ok(())
}

pub fn advertise_service(port: u16) -> Result<ServiceDaemon> {
    let mdns = ServiceDaemon::new()?;
    let service_type = "_acp-ws._tcp.local.";
    let instance_name = format!("acp-ws-{}", port);
    let mut properties = HashMap::new();
    properties.insert("version".to_string(), "1.0".to_string());
    properties.insert("auth".to_string(), "bearer".to_string());
    properties.insert("protocol".to_string(), "acp-ws/1".to_string());

    let service_info = ServiceInfo::new(
        service_type,
        &instance_name,
        "localhost.local.",
        "",
        port,
        Some(properties),
    )?;

    mdns.register(service_info)?;
    Ok(mdns)
}
