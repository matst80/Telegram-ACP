use std::sync::Arc;

use anyhow::Result;
use futures::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;
use tokio_tungstenite::accept_async;
use tokio_tungstenite::tungstenite::Message;

use crate::relay::BroadcastSessionEventSink;

pub async fn run_server(bind_addr: &str, events: Arc<BroadcastSessionEventSink>) -> Result<()> {
    let listener = TcpListener::bind(bind_addr).await?;
    tracing::info!(bind_addr, "Websocket listener started");

    loop {
        let (stream, peer_addr) = listener.accept().await?;
        let events = events.clone();
        tokio::spawn(async move {
            if let Err(err) = handle_connection(stream, events.subscribe()).await {
                tracing::warn!(peer = %peer_addr, "Websocket listener closed: {err}");
            }
        });
    }
}

async fn handle_connection(
    stream: TcpStream,
    mut events: broadcast::Receiver<String>,
) -> Result<()> {
    let websocket = accept_async(stream).await?;
    let (mut writer, mut reader) = websocket.split();

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
                Some(Ok(_)) => {}
                Some(Err(err)) => return Err(err.into()),
            },
        }
    }

    Ok(())
}
