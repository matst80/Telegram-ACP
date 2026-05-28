use std::sync::Arc;

use agent_client_protocol as acp;
use teloxide::prelude::*;
use tokio::sync::{mpsc, Mutex};

use crate::handlers::{EventContext, SessionEventConsumer};
use crate::types::AgentEvent;
use crate::sess_info;

pub async fn run_event_consumer(
    bot: Bot,
    chat_id: ChatId,
    thread_id: i32,
    mut event_rx: mpsc::UnboundedReceiver<AgentEvent>,
    available_commands_cache: Arc<Mutex<Vec<acp::AvailableCommand>>>,
) {
    sess_info!("Event consumer started");
    let mut ctx = EventContext::for_telegram(bot, chat_id, thread_id);
    let mut consumer = SessionEventConsumer::new();

    while let Some(event) = event_rx.recv().await {
        if let AgentEvent::Update(update) = &event {
            if let acp::SessionUpdate::AvailableCommandsUpdate(u) = update.as_ref() {
                *available_commands_cache.lock().await = u.available_commands.clone();
                continue;
            }
        }

        consumer.handle_event(&event, &mut ctx).await;
    }

    consumer.finish(&mut ctx).await;
    sess_info!("Event consumer finished");
}