use teloxide::prelude::*;
use teloxide::types::ParseMode;

use crate::handlers::{EventWriter, NoopEventWriter, OutputRef, TelegramEventWriter};

pub struct EventContext {
    pub writer: Box<dyn EventWriter>,
}

impl EventContext {
    pub fn for_telegram(bot: Bot, chat_id: ChatId, thread_id: i32) -> Self {
        Self {
            writer: Box::new(TelegramEventWriter::new(bot, chat_id, thread_id)),
        }
    }

    pub fn for_telegram_opt(bot: Option<Bot>, chat_id: Option<ChatId>, thread_id: i32) -> Self {
        if let (Some(bot), Some(chat_id)) = (bot, chat_id) {
            Self {
                writer: Box::new(TelegramEventWriter::new(bot, chat_id, thread_id)),
            }
        } else {
            Self {
                writer: Box::new(NoopEventWriter),
            }
        }
    }

    pub async fn send_html(&mut self, text: &str, silent: bool) -> Option<OutputRef> {
        self.writer.send_html(text, silent).await
    }

    pub async fn send_html_drop(&mut self, text: &str, silent: bool) -> Option<OutputRef> {
        self.writer.send_html_drop(text, silent).await
    }

    pub async fn send_html_chunks(&mut self, text: &str, silent: bool) {
        self.writer.send_html_chunks(text, silent).await
    }

    pub async fn send_chunks(&mut self, text: &str, parse_mode: ParseMode, silent: bool) {
        self.writer.send_chunks(text, parse_mode, silent).await
    }

    pub async fn send_draft(&mut self, draft_id: i64, text: &str) -> anyhow::Result<()> {
        self.writer.send_draft(draft_id, text).await
    }

    pub async fn edit_html(&mut self, id: OutputRef, text: &str) -> bool {
        self.writer.edit_html(id, text).await
    }

    pub async fn edit_html_drop(&mut self, id: OutputRef, text: &str) -> bool {
        self.writer.edit_html_drop(id, text).await
    }

    pub async fn delete_msg(&mut self, id: OutputRef) {
        self.writer.delete(id).await
    }

    pub async fn pin_msg(&mut self, id: OutputRef) {
        self.writer.pin(id).await
    }

    pub async fn close_topic(&mut self) {
        self.writer.close_thread().await
    }
}