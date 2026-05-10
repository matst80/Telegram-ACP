use agent_client_protocol as acp;

use super::{EventContext, EventHandler, OutputRef};
use crate::formatting;
use crate::types::AgentEvent;

pub struct PlanHandler {
    message_id: Option<OutputRef>,
}

impl PlanHandler {
    pub fn new() -> Self {
        Self { message_id: None }
    }
}

#[async_trait::async_trait(?Send)]
impl EventHandler for PlanHandler {
    async fn handle(&mut self, event: &AgentEvent, ctx: &mut EventContext) -> bool {
        let plan = match event {
            AgentEvent::Update(update) => match update.as_ref() {
                acp::SessionUpdate::Plan(plan) => plan,
                _ => return false,
            },
            _ => return false,
        };

        if is_plan_completed(plan) {
            if let Some(old_id) = self.message_id.take() {
                ctx.delete_msg(old_id).await;
            }
            ctx.send_html_chunks(&formatting::format_plan_completed(plan), true)
                .await;
            return true;
        }

        let formatted = formatting::format_plan(plan);

        // Try to edit existing plan message
        if let Some(existing_id) = self.message_id {
            if ctx.edit_html(existing_id, &formatted).await {
                return true;
            }
        }

        // Send new plan message and pin it
        if let Some(id) = ctx.send_html(&formatted, true).await {
            self.message_id = Some(id);
            ctx.pin_msg(id).await;
        }
        true
    }
}

fn is_plan_completed(plan: &acp::Plan) -> bool {
    !plan.entries.is_empty()
        && plan
            .entries
            .iter()
            .all(|entry| matches!(entry.status, acp::PlanEntryStatus::Completed))
}
