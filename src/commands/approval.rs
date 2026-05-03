use anyhow::Result;
use async_trait::async_trait;
use teloxide::prelude::*;
use teloxide::types::{InlineKeyboardButton, InlineKeyboardMarkup, MessageId, ThreadId};

use super::{CallbackContext, Command, CommandContext};
use crate::session_control::SessionCommand;
use crate::types::PermissionHandling;

pub struct ApprovalCommand;

#[async_trait]
impl Command for ApprovalCommand {
    fn name(&self) -> &'static str {
        "approval"
    }

    fn description(&self) -> &'static str {
        "Set permission handling (Auto/Manual)"
    }

    async fn execute(&self, ctx: CommandContext<'_>) -> Result<()> {
        let thread_id = ctx.require_thread_id()?;

        let rows = vec![
            vec![InlineKeyboardButton::callback(
                "Auto Approval",
                format!("approval:{}:auto", thread_id),
            )],
            vec![InlineKeyboardButton::callback(
                "Manual Approval",
                format!("approval:{}:manual", thread_id),
            )],
        ];

        let keyboard = InlineKeyboardMarkup::new(rows);
        ctx.bot
            .send_message(ctx.msg.chat.id, "Select permission handling mode:")
            .message_thread_id(ThreadId(MessageId(thread_id)))
            .reply_markup(keyboard)
            .await?;

        Ok(())
    }

    async fn try_handle_callback(&self, ctx: CallbackContext<'_>) -> Result<bool> {
        let data = match ctx.query.data.as_deref() {
            Some(d) => d,
            None => return Ok(false),
        };

        if data.starts_with("approval:") {
            let parts: Vec<&str> = data.split(':').collect();
            if parts.len() != 3 {
                return Ok(false);
            }
            let thread_id = parts[1].parse::<i32>()?;
            let mode = parts[2];

            let handling = match mode {
                "auto" => PermissionHandling::Auto,
                "manual" => PermissionHandling::Manual,
                _ => return Ok(false),
            };

            if let Some(command_tx) = ctx.daemon.get_session_command_tx_by_thread(thread_id) {
                let _ = command_tx.send(SessionCommand::SetPermissionHandling { handling });

                ctx.bot
                    .answer_callback_query(ctx.query.id.clone())
                    .text(format!("Permission handling set to {:?}", handling))
                    .await?;

                if let Some(msg) = ctx.query.message.as_ref() {
                    let _ = ctx
                        .bot
                        .edit_message_text(
                            msg.chat().id,
                            msg.id(),
                            format!("Permission handling: {:?}", handling),
                        )
                        .await;
                }
            } else {
                ctx.bot
                    .answer_callback_query(ctx.query.id.clone())
                    .text("No active session found")
                    .show_alert(true)
                    .await?;
            }
            return Ok(true);
        }

        if data.starts_with("approve:") {
            // handle "approve:request_id:option_id"
            let parts: Vec<&str> = data.split(':').collect();
            if parts.len() != 3 {
                return Ok(false);
            }
            let request_id = parts[1];
            let option_id = parts[2];

            if let Some((_, tx)) = ctx.daemon.pending_permissions.remove(request_id) {
                let _ = tx.send(agent_client_protocol::PermissionOptionId::new(option_id));
                ctx.bot
                    .answer_callback_query(ctx.query.id.clone())
                    .text("Permission granted")
                    .await?;
            } else {
                ctx.bot
                    .answer_callback_query(ctx.query.id.clone())
                    .text("Permission request expired or already handled")
                    .show_alert(true)
                    .await?;
            }
            return Ok(true);
        }

        Ok(false)
    }
}
