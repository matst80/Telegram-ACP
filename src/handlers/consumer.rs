use crate::formatting;
use crate::handlers::{draft, plan, tool_call, working, EventContext};
use crate::types::AgentEvent;

#[async_trait::async_trait(?Send)]
pub trait EventHandler {
    async fn handle(&mut self, event: &AgentEvent, ctx: &mut EventContext) -> bool;
    #[allow(dead_code)]
    async fn finish(&mut self, _ctx: &mut EventContext) {}
    async fn reset(&mut self, _ctx: &mut EventContext) {}
}

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
        if self.draft.handle(event, ctx).await {
            self.working.dismiss(ctx).await;
            return;
        }

        self.draft.flush(ctx).await;

        if !matches!(event, AgentEvent::Working) {
            self.working.dismiss(ctx).await;
        }

        if self.working.handle(event, ctx).await {
            return;
        }
        if self.tool_call.handle(event, ctx).await {
            return;
        }
        if self.plan.handle(event, ctx).await {
            return;
        }

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
    use crate::handlers::{EventContext, EventWriter, OutputRef};
    use std::sync::{Arc, Mutex};
    use teloxide::types::ParseMode;

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

        let mut ctx = EventContext {
            writer: Box::new(mock),
        };
        let mut consumer = SessionEventConsumer::new();

        consumer.handle_event(&AgentEvent::Working, &mut ctx).await;
        {
            let msgs = sent.lock().unwrap();
            assert_eq!(msgs.len(), 1);
            assert!(msgs[0].contains("Working"));
        }

        consumer
            .handle_event(
                &AgentEvent::Finished {
                    content: "done".to_string(),
                },
                &mut ctx,
            )
            .await;

        {
            let del = deleted.lock().unwrap();
            assert_eq!(del.len(), 1);
            assert_eq!(del[0], OutputRef(1));

            let msgs = sent.lock().unwrap();
            assert!(msgs.iter().any(|m| m.contains("done")));
        }
    }
}