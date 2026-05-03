use std::sync::Arc;
use teloxide::net::Download;

use teloxide::prelude::*;
use teloxide::types::{BotCommandScope, CallbackQuery, MessageKind, Recipient};

use crate::commands;
use crate::daemon::DaemonHandle;
use crate::relay::SessionEvent;
use crate::session_control::SessionCommand;

/// Start the Telegram bot dispatcher. Runs until cancelled.
pub async fn run_bot(bot: Bot, daemon: Arc<DaemonHandle>) {
    use teloxide::dispatching::{Dispatcher, UpdateFilterExt};
    use teloxide::dptree;
    use teloxide::types::Update;

    if let Err(e) = register_bot_commands(&bot, daemon.config.chat_id).await {
        tracing::warn!("Failed to register Telegram slash commands: {e}");
    }

    let handler = dptree::entry()
        .branch(Update::filter_message().endpoint(handle_message))
        .branch(Update::filter_callback_query().endpoint(handle_callback_query));

    Dispatcher::builder(bot, handler)
        .dependencies(dptree::deps![daemon])
        .enable_ctrlc_handler()
        .build()
        .dispatch()
        .await;
}

async fn register_bot_commands(bot: &Bot, chat_id: i64) -> anyhow::Result<()> {
    bot.set_my_commands(commands::telegram_menu_commands())
        .scope(BotCommandScope::Chat {
            chat_id: Recipient::Id(ChatId(chat_id)),
        })
        .await?;
    Ok(())
}

/// Handle an incoming Telegram message.
async fn handle_message(bot: Bot, msg: Message, daemon: Arc<DaemonHandle>) -> anyhow::Result<()> {
    if msg.chat.id != ChatId(daemon.config.chat_id) {
        return Ok(());
    }

    if msg.date < daemon.start_time {
        tracing::debug!(
            "Skipping old message sent at {} (daemon started at {})",
            msg.date,
            daemon.start_time
        );
        return Ok(());
    }

    if commands::execute_slash_command(&bot, &msg, &daemon).await? {
        return Ok(());
    }

    if let Some(thread_id) = msg.thread_id {
        if msg.from.as_ref().map(|user| user.is_bot).unwrap_or(false) {
            return Ok(());
        }

        if let Err(e) = bot
            .pin_chat_message(msg.chat.id, msg.id)
            .disable_notification(true)
            .await
        {
            tracing::warn!(
                chat_id = msg.chat.id.0,
                thread_id = thread_id.0 .0,
                message_id = msg.id.0,
                "Failed to pin user message: {e}"
            );
        }

        let mut content = Vec::new();

        // 1. Check for photos
        if let Some(photos) = msg.photo() {
            if let Some(photo) = photos.last() {
                match download_image_as_base64(&bot, photo.file.id.clone()).await {
                    Ok((data, mime_type)) => {
                        content.push(agent_client_protocol::ContentBlock::Image(
                            agent_client_protocol::ImageContent::new(data, mime_type),
                        ));
                    }
                    Err(e) => {
                        tracing::warn!("Failed to download image: {e}");
                    }
                }
            }
        }

        // 2. Handle text/caption
        if let Some(text) = msg.text().or_else(|| msg.caption()) {
            let prompt = build_prompt_with_quote(&msg, text);
            content.push(agent_client_protocol::ContentBlock::Text(
                agent_client_protocol::TextContent::new(prompt),
            ));
        }

        if !content.is_empty() {
            handle_topic_message(content, thread_id, &daemon).await?;
        }
    }

    Ok(())
}

/// Handle a message in a forum topic, routing it to the corresponding agent session.
async fn handle_topic_message(
    content: Vec<agent_client_protocol::ContentBlock>,
    thread_id: teloxide::types::ThreadId,
    daemon: &DaemonHandle,
) -> anyhow::Result<()> {
    let thread = thread_id.0 .0;
    let Some(command_tx) = daemon.get_session_command_tx_by_thread(thread) else {
        return Ok(());
    };

    let text = crate::session::extract_text_from_content(&content);

    daemon
        .session_event_sink
        .publish(SessionEvent::UserPrompt {
            thread_id: Some(thread),
            acp_session_id: daemon.get_acp_session_id_by_thread(thread),
            text,
            content: content.clone(),
        })
        .await;

    command_tx
        .send(SessionCommand::Prompt(content))
        .map_err(|_| anyhow::anyhow!("Session command channel closed"))?;

    Ok(())
}

async fn handle_callback_query(
    bot: Bot,
    query: CallbackQuery,
    daemon: Arc<DaemonHandle>,
) -> anyhow::Result<()> {
    commands::handle_callback_query(bot, query, daemon).await
}

fn build_prompt_with_quote(msg: &Message, raw_text: &str) -> String {
    let Some(quote_text) = extract_quote_text(msg) else {
        return raw_text.to_string();
    };

    format!("{}\n\n{}", format_blockquote(&quote_text), raw_text)
}

fn extract_quote_text(msg: &Message) -> Option<String> {
    let MessageKind::Common(common) = &msg.kind else {
        return None;
    };

    if let Some(quote) = &common.quote {
        if !quote.text.trim().is_empty() {
            return Some(quote.text.clone());
        }
    }

    let reply = common.reply_to_message.as_deref()?;
    let text = reply.text().or_else(|| reply.caption())?;
    let trimmed = text.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

fn format_blockquote(text: &str) -> String {
    text.lines()
        .map(|line| format!("> {line}"))
        .collect::<Vec<_>>()
        .join("\n")
}

async fn download_image_as_base64(
    bot: &Bot,
    file_id: teloxide::types::FileId,
) -> anyhow::Result<(String, String)> {
    use base64::prelude::*;

    let file = bot.get_file(file_id).await?;
    let mut buf = Vec::new();
    bot.download_file(&file.path, &mut buf).await?;

    // Infer mime type from extension or default to image/jpeg
    let mime_type = if file.path.ends_with(".png") {
        "image/png"
    } else if file.path.ends_with(".webp") {
        "image/webp"
    } else if file.path.ends_with(".gif") {
        "image/gif"
    } else {
        "image/jpeg"
    };

    let encoded = BASE64_STANDARD.encode(buf);
    Ok((encoded, mime_type.to_string()))
}
