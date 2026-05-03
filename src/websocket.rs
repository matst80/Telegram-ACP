use std::sync::Arc;
use std::collections::HashMap;
use mdns_sd::{ServiceDaemon, ServiceInfo};

use anyhow::Result;
use futures::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;
use tokio_tungstenite::accept_async;
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
    let websocket = accept_async(stream).await?;
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
