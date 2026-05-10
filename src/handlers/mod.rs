//! Handlers for ACP events/Agent updates

pub mod draft;
pub mod plan;
pub mod tool_call;
pub mod working;

use std::future::Future;

use teloxide::prelude::*;
use teloxide::types::{MessageId, ParseMode, ThreadId};
use teloxide::RequestError;
use tokio::time::{sleep_until, Duration, Instant};

use crate::formatting;
use crate::sess_warn;
use crate::types::AgentEvent;

// --- OutboundThrottle (moved from session.rs) ---

pub struct OutboundThrottle {
    min_interval: Duration,
    next_allowed_at: Instant,
}

impl OutboundThrottle {
    pub fn with_interval(secs: f64) -> Self {
        let min_interval = Duration::from_secs_f64(secs);
        Self {
            min_interval,
            next_allowed_at: Instant::now(),
        }
    }

    pub async fn wait_turn(&mut self) {
        let now = Instant::now();
        if self.next_allowed_at > now {
            sleep_until(self.next_allowed_at).await;
        }
        self.next_allowed_at = Instant::now() + self.min_interval;
    }

    pub fn try_turn(&mut self) -> bool {
        let now = Instant::now();
        if self.next_allowed_at > now {
            return false;
        }
        self.next_allowed_at = now + self.min_interval;
        true
    }

    pub fn defer_for(&mut self, delay: Duration) {
        let retry_at = Instant::now() + delay;
        if retry_at > self.next_allowed_at {
            self.next_allowed_at = retry_at;
        }
    }
}

// --- EventWriter trait ---

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct OutputRef(pub i32);

impl From<MessageId> for OutputRef {
    fn from(id: MessageId) -> Self {
        Self(id.0)
    }
}

impl From<OutputRef> for MessageId {
    fn from(id: OutputRef) -> Self {
        Self(id.0)
    }
}

#[async_trait::async_trait(?Send)]
pub trait EventWriter {
    async fn send_html(&mut self, text: &str, silent: bool) -> Option<OutputRef>;
    async fn send_html_drop(&mut self, text: &str, silent: bool) -> Option<OutputRef>;
    async fn edit_html(&mut self, id: OutputRef, text: &str) -> bool;
    async fn edit_html_drop(&mut self, id: OutputRef, text: &str) -> bool;
    async fn delete(&mut self, id: OutputRef);
    async fn pin(&mut self, id: OutputRef);
    async fn close_thread(&mut self);
    async fn send_html_chunks(&mut self, text: &str, silent: bool);
    async fn send_chunks(&mut self, text: &str, parse_mode: ParseMode, silent: bool);
    async fn send_draft(&mut self, draft_id: i64, text: &str) -> anyhow::Result<()>;
}

// --- TelegramEventWriter ---

pub struct TelegramEventWriter {
    bot: Bot,
    chat_id: ChatId,
    thread_id: i32,
    throttle: OutboundThrottle,
}

impl TelegramEventWriter {
    const RETRY_AFTER_PADDING: Duration = Duration::from_secs(1);

    pub fn new(bot: Bot, chat_id: ChatId, thread_id: i32) -> Self {
        Self {
            bot,
            chat_id,
            thread_id,
            throttle: OutboundThrottle::with_interval(2.0),
        }
    }

    fn html_fallback_text(text: &str) -> Option<String> {
        if text.len() <= 4096 {
            None
        } else {
            Some(formatting::truncate_message(text, 3900))
        }
    }

    async fn request_with_throttle<T, F, Fut>(
        &mut self,
        label: &str,
        mut request: F,
    ) -> Result<T, RequestError>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, RequestError>>,
    {
        loop {
            self.throttle.wait_turn().await;
            match request().await {
                Ok(value) => return Ok(value),
                Err(RequestError::RetryAfter(after)) => {
                    let delay = after.duration() + Self::RETRY_AFTER_PADDING;
                    self.throttle.defer_for(delay);
                    sess_warn!(
                        "Telegram rate limit while {label}; backing off for {}s before retrying",
                        delay.as_secs()
                    );
                }
                Err(err) => return Err(err),
            }
        }
    }

    async fn request_with_throttle_drop<T, F, Fut>(
        &mut self,
        label: &str,
        mut request: F,
    ) -> Result<Option<T>, RequestError>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T, RequestError>>,
    {
        if !self.throttle.try_turn() {
            return Ok(None);
        }
        match request().await {
            Ok(value) => Ok(Some(value)),
            Err(RequestError::RetryAfter(after)) => {
                let delay = after.duration() + Self::RETRY_AFTER_PADDING;
                self.throttle.defer_for(delay);
                sess_warn!(
                    "Telegram rate limit while {label}; backing off for {}s (dropping update)",
                    delay.as_secs()
                );
                Ok(None)
            }
            Err(err) => Err(err),
        }
    }
}

#[async_trait::async_trait(?Send)]
impl EventWriter for TelegramEventWriter {
    async fn send_html(&mut self, text: &str, silent: bool) -> Option<OutputRef> {
        if let Some(text) = Self::html_fallback_text(text) {
            let bot = self.bot.clone();
            let chat_id = self.chat_id;
            let thread_id = self.thread_id;
            match self
                .request_with_throttle(
                    "sending plain-text fallback message to Telegram",
                    move || {
                        bot.send_message(chat_id, text.clone())
                            .message_thread_id(ThreadId(MessageId(thread_id)))
                            .disable_notification(silent)
                            .send()
                    },
                )
                .await
            {
                Ok(message) => Some(message.id.into()),
                Err(err) => {
                    sess_warn!("Failed to send plain-text fallback message to Telegram: {err}");
                    None
                }
            }
        } else {
            let bot = self.bot.clone();
            let chat_id = self.chat_id;
            let thread_id = self.thread_id;
            let text = text.to_string();
            match self
                .request_with_throttle("sending HTML message to Telegram", move || {
                    bot.send_message(chat_id, text.clone())
                        .message_thread_id(ThreadId(MessageId(thread_id)))
                        .parse_mode(ParseMode::Html)
                        .disable_notification(silent)
                        .send()
                })
                .await
            {
                Ok(message) => Some(message.id.into()),
                Err(err) => {
                    sess_warn!("Failed to send HTML message to Telegram: {err}");
                    None
                }
            }
        }
    }

    async fn send_html_drop(&mut self, text: &str, silent: bool) -> Option<OutputRef> {
        if let Some(text) = Self::html_fallback_text(text) {
            let bot = self.bot.clone();
            let chat_id = self.chat_id;
            let thread_id = self.thread_id;
            match self
                .request_with_throttle_drop(
                    "sending plain-text fallback message to Telegram",
                    move || {
                        bot.send_message(chat_id, text.clone())
                            .message_thread_id(ThreadId(MessageId(thread_id)))
                            .disable_notification(silent)
                            .send()
                    },
                )
                .await
            {
                Ok(Some(message)) => Some(message.id.into()),
                Ok(None) => None,
                Err(err) => {
                    sess_warn!("Failed to send plain-text fallback message to Telegram: {err}");
                    None
                }
            }
        } else {
            let bot = self.bot.clone();
            let chat_id = self.chat_id;
            let thread_id = self.thread_id;
            let text = text.to_string();
            match self
                .request_with_throttle_drop("sending HTML message to Telegram", move || {
                    bot.send_message(chat_id, text.clone())
                        .message_thread_id(ThreadId(MessageId(thread_id)))
                        .parse_mode(ParseMode::Html)
                        .disable_notification(silent)
                        .send()
                })
                .await
            {
                Ok(Some(message)) => Some(message.id.into()),
                Ok(None) => None,
                Err(err) => {
                    sess_warn!("Failed to send HTML message to Telegram: {err}");
                    None
                }
            }
        }
    }

    async fn edit_html(&mut self, id: OutputRef, text: &str) -> bool {
        let msg_id = MessageId::from(id);
        if let Some(text) = Self::html_fallback_text(text) {
            let bot = self.bot.clone();
            let chat_id = self.chat_id;
            match self
                .request_with_throttle(
                    &format!(
                        "editing Telegram message {} with plain-text fallback",
                        msg_id.0
                    ),
                    move || bot.edit_message_text(chat_id, msg_id, text.clone()).send(),
                )
                .await
            {
                Ok(_) => true,
                Err(err) => {
                    sess_warn!(
                        "Failed to edit Telegram message {} with plain-text fallback: {}",
                        msg_id.0,
                        err
                    );
                    false
                }
            }
        } else {
            let bot = self.bot.clone();
            let chat_id = self.chat_id;
            let text = text.to_string();
            match self
                .request_with_throttle(
                    &format!("editing Telegram message {}", msg_id.0),
                    move || {
                        bot.edit_message_text(chat_id, msg_id, text.clone())
                            .parse_mode(ParseMode::Html)
                            .send()
                    },
                )
                .await
            {
                Ok(_) => true,
                Err(err) => {
                    sess_warn!("Failed to edit Telegram message {}: {}", msg_id.0, err);
                    false
                }
            }
        }
    }

    async fn edit_html_drop(&mut self, id: OutputRef, text: &str) -> bool {
        let msg_id = MessageId::from(id);
        if let Some(text) = Self::html_fallback_text(text) {
            let bot = self.bot.clone();
            let chat_id = self.chat_id;
            match self
                .request_with_throttle_drop(
                    &format!(
                        "editing Telegram message {} with plain-text fallback",
                        msg_id.0
                    ),
                    move || bot.edit_message_text(chat_id, msg_id, text.clone()).send(),
                )
                .await
            {
                Ok(Some(_)) => true,
                Ok(None) => false,
                Err(err) => {
                    sess_warn!(
                        "Failed to edit Telegram message {} with plain-text fallback: {}",
                        msg_id.0,
                        err
                    );
                    false
                }
            }
        } else {
            let bot = self.bot.clone();
            let chat_id = self.chat_id;
            let text = text.to_string();
            match self
                .request_with_throttle_drop(
                    &format!("editing Telegram message {}", msg_id.0),
                    move || {
                        bot.edit_message_text(chat_id, msg_id, text.clone())
                            .parse_mode(ParseMode::Html)
                            .send()
                    },
                )
                .await
            {
                Ok(Some(_)) => true,
                Ok(None) => false,
                Err(err) => {
                    sess_warn!("Failed to edit Telegram message {}: {}", msg_id.0, err);
                    false
                }
            }
        }
    }

    async fn delete(&mut self, id: OutputRef) {
        let msg_id = MessageId::from(id);
        let bot = self.bot.clone();
        let chat_id = self.chat_id;
        if let Err(err) = self
            .request_with_throttle(
                &format!("deleting Telegram message {}", msg_id.0),
                move || bot.delete_message(chat_id, msg_id).send(),
            )
            .await
        {
            sess_warn!("Failed to delete Telegram message {}: {}", msg_id.0, err);
        }
    }

    async fn pin(&mut self, id: OutputRef) {
        let msg_id = MessageId::from(id);
        let bot = self.bot.clone();
        let chat_id = self.chat_id;
        if let Err(e) = self
            .request_with_throttle(
                &format!("pinning Telegram message {}", msg_id.0),
                move || {
                    bot.pin_chat_message(chat_id, msg_id)
                        .disable_notification(true)
                        .send()
                },
            )
            .await
        {
            sess_warn!("Failed to pin Telegram message {}: {}", msg_id.0, e);
        }
    }

    async fn close_thread(&mut self) {
        let bot = self.bot.clone();
        let chat_id = self.chat_id;
        let thread_id = self.thread_id;
        let _ = self
            .request_with_throttle("closing Telegram topic", move || {
                bot.close_forum_topic(chat_id, ThreadId(MessageId(thread_id)))
                    .send()
            })
            .await
            .map_err(|err| sess_warn!("Failed to close Telegram topic: {err}"));
    }

    async fn send_html_chunks(&mut self, text: &str, silent: bool) {
        let chunks = formatting::split_message(text, 4096);
        for chunk in chunks {
            let bot = self.bot.clone();
            let chat_id = self.chat_id;
            let thread_id = self.thread_id;
            let _ = self
                .request_with_throttle("sending HTML message chunk to Telegram", move || {
                    bot.send_message(chat_id, chunk.clone())
                        .message_thread_id(ThreadId(MessageId(thread_id)))
                        .parse_mode(ParseMode::Html)
                        .disable_notification(silent)
                        .send()
                })
                .await
                .map_err(|err| sess_warn!("Failed to send HTML message chunk to Telegram: {err}"));
        }
    }

    async fn send_chunks(&mut self, text: &str, parse_mode: ParseMode, silent: bool) {
        let chunks = formatting::split_message(text, 4096);
        for chunk in chunks {
            let bot = self.bot.clone();
            let chat_id = self.chat_id;
            let thread_id = self.thread_id;
            let _ = self
                .request_with_throttle("sending Telegram message chunk", move || {
                    bot.send_message(chat_id, chunk.clone())
                        .message_thread_id(ThreadId(MessageId(thread_id)))
                        .parse_mode(parse_mode)
                        .disable_notification(silent)
                        .send()
                })
                .await
                .map_err(|err| sess_warn!("Failed to send Telegram message chunk: {err}"));
        }
    }

    async fn send_draft(&mut self, draft_id: i64, text: &str) -> anyhow::Result<()> {
        if !self.throttle.try_turn() {
            return Ok(());
        }
        let client = self.bot.client();
        let token = self.bot.token();
        let url = format!("https://api.telegram.org/bot{token}/sendMessageDraft");
        let telegram_text = formatting::markdown_to_telegram_md_v2(text);
        let draft_text = formatting::truncate_message(&telegram_text, 4096);

        let mut body = serde_json::json!({
            "chat_id": self.chat_id.0,
            "draft_id": draft_id,
            "text": draft_text,
            "parse_mode": "MarkdownV2",
        });

        if self.thread_id != 0 {
            body["message_thread_id"] = serde_json::json!(self.thread_id);
        }

        let resp = client.post(&url).json(&body).send().await?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body_text = resp.text().await.unwrap_or_default();
            anyhow::bail!("sendMessageDraft failed ({status}): {body_text}");
        }
        Ok(())
    }
}

// --- EventContext ---

/// Shared context passed to all handlers for sending messages.
pub struct EventContext {
    pub writer: Box<dyn EventWriter>,
}

impl EventContext {
    pub fn for_telegram(bot: Bot, chat_id: ChatId, thread_id: i32) -> Self {
        Self {
            writer: Box::new(TelegramEventWriter::new(bot, chat_id, thread_id)),
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

// --- EventHandler trait ---

#[async_trait::async_trait(?Send)]
pub trait EventHandler {
    /// Process an event. Return true if consumed.
    async fn handle(&mut self, event: &AgentEvent, ctx: &mut EventContext) -> bool;
    /// Called when the event stream ends.
    #[allow(dead_code)]
    async fn finish(&mut self, _ctx: &mut EventContext) {}
    /// Called on turn boundaries (Finished/Error) to reset per-turn state.
    async fn reset(&mut self, _ctx: &mut EventContext) {}
}

// --- SessionEventConsumer ---

pub struct SessionEventConsumer {
    draft: draft::DraftHandler,
    working: working::WorkingHandler,
    tool_call: tool_call::ToolCallHandler,
    plan: plan::PlanHandler,
}

impl SessionEventConsumer {
    pub fn new() -> Self {
        Self {
            draft: draft::DraftHandler::new(),
            working: working::WorkingHandler::new(),
            tool_call: tool_call::ToolCallHandler::new(),
            plan: plan::PlanHandler::new(),
        }
    }

    pub async fn handle_event(&mut self, event: &AgentEvent, ctx: &mut EventContext) {
        // Text chunks → draft handler (streaming)
        if self.draft.handle(event, ctx).await {
            self.working.dismiss(ctx).await;
            return;
        }

        // Non-text event: flush accumulated draft
        self.draft.flush(ctx).await;

        // Dismiss working indicator for non-Working events
        if !matches!(event, AgentEvent::Working) {
            self.working.dismiss(ctx).await;
        }

        // Dispatch to handlers
        if self.working.handle(event, ctx).await {
            return;
        }
        if self.tool_call.handle(event, ctx).await {
            return;
        }
        if self.plan.handle(event, ctx).await {
            return;
        }

        // Inline: simple events
        match event {
            AgentEvent::Update(_update) => {}
            AgentEvent::Finished { content } => {
                ctx.send_html_chunks(&formatting::format_completion(content, None), false)
                    .await;
                self.tool_call.reset(ctx).await;
            }
            AgentEvent::Error { content } => {
                ctx.send_html_chunks(&formatting::format_error(content), false)
                    .await;
                self.tool_call.reset(ctx).await;
            }
            _ => {}
        }
    }

    pub async fn finish(&mut self, ctx: &mut EventContext) {
        self.draft.flush(ctx).await;
        ctx.close_topic().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    #[derive(Default, Clone)]
    struct MockEventWriter {
        pub sent_messages: Arc<Mutex<Vec<String>>>,
        pub deleted_messages: Arc<Mutex<Vec<OutputRef>>>,
        pub pins: Arc<Mutex<Vec<OutputRef>>>,
    }

    #[async_trait::async_trait(?Send)]
    impl EventWriter for MockEventWriter {
        async fn send_html(&mut self, text: &str, _silent: bool) -> Option<OutputRef> {
            self.sent_messages.lock().unwrap().push(text.to_string());
            Some(OutputRef(self.sent_messages.lock().unwrap().len() as i32))
        }
        async fn send_html_drop(&mut self, text: &str, _silent: bool) -> Option<OutputRef> {
            self.sent_messages.lock().unwrap().push(text.to_string());
            Some(OutputRef(self.sent_messages.lock().unwrap().len() as i32))
        }
        async fn edit_html(&mut self, _id: OutputRef, text: &str) -> bool {
            self.sent_messages.lock().unwrap().push(format!("edit: {}", text));
            true
        }
        async fn edit_html_drop(&mut self, _id: OutputRef, text: &str) -> bool {
            self.sent_messages.lock().unwrap().push(format!("edit: {}", text));
            true
        }
        async fn delete(&mut self, id: OutputRef) {
            self.deleted_messages.lock().unwrap().push(id);
        }
        async fn pin(&mut self, id: OutputRef) {
            self.pins.lock().unwrap().push(id);
        }
        async fn close_thread(&mut self) {}
        async fn send_html_chunks(&mut self, text: &str, _silent: bool) {
            self.sent_messages.lock().unwrap().push(text.to_string());
        }
        async fn send_chunks(&mut self, text: &str, _mode: ParseMode, _silent: bool) {
            self.sent_messages.lock().unwrap().push(text.to_string());
        }
        async fn send_draft(&mut self, _id: i64, text: &str) -> anyhow::Result<()> {
            self.sent_messages.lock().unwrap().push(format!("draft: {}", text));
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_session_event_consumer_working_indicator() {
        let mock = MockEventWriter::default();
        let sent = mock.sent_messages.clone();
        let deleted = mock.deleted_messages.clone();
        
        let mut ctx = EventContext { writer: Box::new(mock) };
        let mut consumer = SessionEventConsumer::new();

        // 1. Send Working event
        consumer.handle_event(&AgentEvent::Working, &mut ctx).await;
        {
            let msgs = sent.lock().unwrap();
            assert_eq!(msgs.len(), 1);
            assert!(msgs[0].contains("Working"));
        }

        // 2. Send some other event (e.g. Finished) to trigger dismissal
        consumer.handle_event(&AgentEvent::Finished { content: "done".to_string() }, &mut ctx).await;
        
        {
            // Working indicator should be deleted
            let del = deleted.lock().unwrap();
            assert_eq!(del.len(), 1);
            assert_eq!(del[0], OutputRef(1));
            
            // Completion message should be sent
            let msgs = sent.lock().unwrap();
            assert!(msgs.iter().any(|m| m.contains("done")));
        }
    }
}
