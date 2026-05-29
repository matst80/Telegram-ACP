use std::path::PathBuf;
use teloxide::prelude::*;
use crate::relay::{SessionEvent, SessionStateProvider};
use crate::session_control::SessionCommand;
use crate::session_manager::TopicEntry;
use crate::daemon::DaemonHandle;

#[async_trait::async_trait]
impl crate::relay::WebSocketCommandHandler for DaemonHandle {
    async fn handle_command(&self, command: crate::relay::WebSocketCommand) -> anyhow::Result<()> {
        match command {
            crate::relay::WebSocketCommand::SendPrompt {
                thread_id,
                session_id,
                text,
            } => {
                let tx = self
                    .session_manager
                    .resolve_session_tx(thread_id, session_id.clone())
                    .ok_or_else(|| anyhow::anyhow!("No active session found"))?;

                // Publish UserPrompt event to history using the session's sink
                let resolved_tid = self
                    .session_manager
                    .resolve_thread_id(thread_id, session_id.clone());
                let resolved_sid = session_id.or_else(|| {
                    resolved_tid
                        .and_then(|tid| self.session_manager.get_acp_session_id_by_thread(tid))
                });

                let content = vec![agent_client_protocol::ContentBlock::Text(
                    agent_client_protocol::TextContent::new(text.clone()),
                )];

                if let Some(tid) = resolved_tid {
                    if let Some(sink) = self.session_manager.get_session_event_sink_by_thread(tid) {
                        sink.publish(SessionEvent::UserPrompt {
                            thread_id: Some(tid),
                            acp_session_id: resolved_sid,
                            text,
                            content: content.clone(),
                        })
                        .await;
                    }
                } else {
                    // Fallback to global sink if no thread context (shouldn't happen for active sessions)
                    self.session_event_sink
                        .publish(SessionEvent::UserPrompt {
                            thread_id: None,
                            acp_session_id: resolved_sid,
                            text,
                            content: content.clone(),
                        })
                        .await;
                }

                tx.send(SessionCommand::Prompt(content))
                    .map_err(|_| anyhow::anyhow!("Failed to send prompt to session"))?;
            }
            crate::relay::WebSocketCommand::Cancel {
                thread_id,
                session_id,
            } => {
                let cancel_tx = self
                    .session_manager
                    .resolve_session_cancel_tx(thread_id, session_id)
                    .ok_or_else(|| anyhow::anyhow!("No active session found"))?;
                let (result_tx, result_rx) = tokio::sync::oneshot::channel();
                cancel_tx
                    .send(result_tx)
                    .map_err(|_| anyhow::anyhow!("Session cancel channel closed"))?;
                result_rx
                    .await
                    .map_err(|_| anyhow::anyhow!("Cancel request dropped"))??;
            }
            crate::relay::WebSocketCommand::SetConfigOption {
                thread_id,
                session_id,
                config_id,
                value_id,
            } => {
                let tx = self
                    .session_manager
                    .resolve_session_tx(thread_id, session_id)
                    .ok_or_else(|| anyhow::anyhow!("No active session found"))?;
                let (result_tx, result_rx) = tokio::sync::oneshot::channel();
                tx.send(SessionCommand::SetConfigOption {
                    config_id,
                    value_id,
                    result_tx,
                })
                .map_err(|_| anyhow::anyhow!("Failed to send config change to session"))?;
                result_rx.await??;
            }
            crate::relay::WebSocketCommand::SetPermissionMode {
                thread_id,
                session_id,
                mode_id,
            } => {
                let tx = self
                    .session_manager
                    .resolve_session_tx(thread_id, session_id)
                    .ok_or_else(|| anyhow::anyhow!("No active session found"))?;
                let (result_tx, result_rx) = tokio::sync::oneshot::channel();
                tx.send(SessionCommand::SetPermissionMode { mode_id, result_tx })
                    .map_err(|_| anyhow::anyhow!("Failed to send permission change to session"))?;
                result_rx.await??;
            }
            crate::relay::WebSocketCommand::SpawnSession {
                project_path,
                agent_command,
                thread_id,
                _metadata: _,
            } => {
                let resolved_thread_id = thread_id.unwrap_or_else(|| {
                    // Generate unique negative thread ID for headless session
                    -1 - (self.session_manager.topics.len() as i32)
                });
                let path = self.resolve_project_path(PathBuf::from(&project_path));
                let (agent_name, agent_cmd) =
                    self.config.resolve_agent(agent_command.as_deref())?;
                self.enqueue_start_session(
                    resolved_thread_id,
                    path,
                    agent_cmd,
                    Some(agent_name),
                    None,
                    false,
                    Vec::new(),
                )
                .await?;
            }
            crate::relay::WebSocketCommand::EndSession {
                session_id,
                thread_id,
            } => {
                let cancel_tx = self
                    .session_manager
                    .resolve_session_cancel_tx(thread_id, session_id.clone())
                    .ok_or_else(|| anyhow::anyhow!("No active session found"))?;
                let (result_tx, result_rx) = tokio::sync::oneshot::channel();
                let _ = cancel_tx.send(result_tx);
                let _ = result_rx.await;

                // Also remove it from topics if it's headless (negative ID)
                if let Some(tid) = thread_id {
                    if tid < 0 {
                        if let Some((_, entry)) = self.session_manager.topics.remove(&tid) {
                            let acp_session_id = entry.active.as_ref().and_then(|a| a.acp_session_id.clone());
                            self.session_event_sink
                                .publish(SessionEvent::SessionRemoved {
                                    thread_id: tid,
                                    acp_session_id,
                                })
                                .await;
                        }
                    }
                } else if let Some(sid) = session_id {
                    let mut to_remove = None;
                    for entry in self.session_manager.topics.iter() {
                        if let Some(active) = &entry.value().active {
                            if active.acp_session_id.as_deref() == Some(&sid) {
                                to_remove = Some(*entry.key());
                                break;
                            }
                        }
                    }
                    if let Some(tid) = to_remove {
                        if tid < 0 {
                            if let Some((_, entry)) = self.session_manager.topics.remove(&tid) {
                                let acp_session_id = entry.active.as_ref().and_then(|a| a.acp_session_id.clone());
                                self.session_event_sink
                                    .publish(SessionEvent::SessionRemoved {
                                        thread_id: tid,
                                        acp_session_id,
                                    })
                                    .await;
                            }
                        }
                    }
                }
            }
            crate::relay::WebSocketCommand::PermissionResponse {
                request_id,
                decision,
            } => {
                if let Some((_, tx)) = self.pending_permissions.remove(&request_id) {
                    let _ = tx.send(agent_client_protocol::PermissionOptionId::new(decision));
                }
            }
            crate::relay::WebSocketCommand::BindTelegramThread {
                session_id,
                thread_id,
                name,
            } => {
                let sid = session_id
                    .clone()
                    .ok_or_else(|| anyhow::anyhow!("session_id required"))?;

                // Validation rules (daemon-side)
                if thread_id.is_none() || thread_id.unwrap() <= 0 {
                    if let Some(n) = &name {
                        let trimmed = n.trim();
                        if trimmed.is_empty() {
                            self.session_event_sink
                                .publish(SessionEvent::Error {
                                    in_reply_to: Some("bind_telegram_thread".to_string()),
                                    acp_session_id: session_id.clone(),
                                    thread_id: thread_id.clone(),
                                    code: "name_required".to_string(),
                                    message: "name field required when thread_id is null"
                                        .to_string(),
                                })
                                .await;
                            return Ok(());
                        }
                        if trimmed.len() > 128 {
                            self.session_event_sink
                                .publish(SessionEvent::Error {
                                    in_reply_to: Some("bind_telegram_thread".to_string()),
                                    acp_session_id: session_id.clone(),
                                    thread_id: thread_id.clone(),
                                    code: "invalid_argument".to_string(),
                                    message: "name too long (max 128 chars)".to_string(),
                                })
                                .await;
                            return Ok(());
                        }
                        if trimmed.chars().any(|c| c.is_control()) {
                            self.session_event_sink
                                .publish(SessionEvent::Error {
                                    in_reply_to: Some("bind_telegram_thread".to_string()),
                                    acp_session_id: session_id.clone(),
                                    thread_id: thread_id.clone(),
                                    code: "invalid_argument".to_string(),
                                    message: "name contains control characters".to_string(),
                                })
                                .await;
                            return Ok(());
                        }
                    } else {
                        // Migration fallback
                        tracing::warn!("bind_telegram_thread: name missing when thread_id is null. Falling back to project name (deprecated).");
                    }
                }

                let mut old_key = None;
                for entry in self.session_manager.topics.iter() {
                    if let Some(active) = &entry.value().active {
                        if active.acp_session_id.as_deref() == Some(&sid) {
                            old_key = Some(*entry.key());
                            break;
                        }
                    }
                }
                let okey = old_key.ok_or_else(|| anyhow::anyhow!("No active session found"))?;

                let mut created = false;
                let (resolved_tid, resolved_name) = match thread_id {
                    Some(tid) if tid > 0 => (tid, name.clone().unwrap_or_default()),
                    _ => {
                        created = true;
                        let topic_name = if let Some(n) = name.as_ref() {
                            n.trim().to_string()
                        } else {
                            let folder_name = {
                                let topics = self.session_manager.topics.get(&okey).unwrap();
                                let active = topics.active.as_ref().unwrap();
                                active
                                    .project_path
                                    .file_name()
                                    .map(|n| n.to_string_lossy().to_string())
                                    .unwrap_or_else(|| "project".to_string())
                            };
                            format!("{}: {}", folder_name, Self::generate_two_words())
                        };
                        let topic = self
                            .bot
                            .create_forum_topic(ChatId(self.config.chat_id), &topic_name)
                            .icon_color(teloxide::types::Rgb::from_u32(0x6FB9F0))
                            .await?;
                        (topic.thread_id.0 .0, topic_name)
                    }
                };

                if let Some((_, mut topic_entry)) = self.session_manager.topics.remove(&okey) {
                    if let Some(active) = topic_entry.active.take() {
                        active
                            .telegram_thread_id
                            .store(resolved_tid, std::sync::atomic::Ordering::Relaxed);
                        *active.name.lock().await = Some(resolved_name.clone());

                        let mut new_topic = self
                            .session_manager
                            .topics
                            .entry(resolved_tid)
                            .or_insert_with(|| TopicEntry {
                                name: Some(resolved_name.clone()),
                                active: None,
                                history: Vec::new(),
                            });
                        new_topic.name = Some(resolved_name.clone());
                        new_topic.active = Some(active);
                        drop(new_topic);

                        // Emit event
                        self.session_event_sink
                            .publish(SessionEvent::TelegramThreadBound {
                                acp_session_id: sid,
                                thread_id: resolved_tid,
                                name: resolved_name,
                                created,
                            })
                            .await;
                    }
                }
            }
            crate::relay::WebSocketCommand::RenameSession {
                thread_id,
                session_id,
                name,
            } => {
                let tid = self
                    .session_manager
                    .resolve_thread_id(thread_id, session_id.clone());
                let mut sid = session_id;
                let mut event_sink = self.session_event_sink.clone();

                if let Some(tid) = tid {
                    if let Some(mut topic) = self.session_manager.topics.get_mut(&tid) {
                        topic.name = Some(name.clone());
                        if let Some(active) = &topic.active {
                            *active.name.lock().await = Some(name.clone());
                            if sid.is_none() {
                                sid = active.acp_session_id.clone();
                            }
                            event_sink = active.event_sink.clone();
                        }
                    }

                    let resolved_sid = sid.unwrap_or_default();

                    event_sink
                        .publish(SessionEvent::SessionRenamed {
                            thread_id: tid,
                            acp_session_id: resolved_sid,
                            name: name.clone(),
                        })
                        .await;

                    self.session_manager.persist_topics().await;

                    // Rename telegram topic if it exists
                    if tid > 0 {
                        let _ = self
                            .bot
                            .edit_forum_topic(
                                ChatId(self.config.chat_id),
                                teloxide::types::ThreadId(teloxide::types::MessageId(tid)),
                            )
                            .name(&name)
                            .await;
                    }
                }
            }
            crate::relay::WebSocketCommand::RemoveTopic { thread_id } => {
                if self.remove_topic(thread_id).await.is_some() {
                    self.session_event_sink
                        .publish(SessionEvent::TopicRemoved { thread_id })
                        .await;
                }
            }
            crate::relay::WebSocketCommand::ExecuteCommand {
                thread_id,
                session_id,
                command_id,
                arguments,
            } => {
                let tx = self
                    .session_manager
                    .resolve_session_tx(thread_id, session_id)
                    .ok_or_else(|| anyhow::anyhow!("No active session found"))?;
                let (result_tx, result_rx) = tokio::sync::oneshot::channel();
                tx.send(SessionCommand::ExecuteCommand {
                    command_id,
                    arguments,
                    result_tx,
                })
                .map_err(|_| anyhow::anyhow!("Failed to send command to session"))?;
                result_rx.await??;
            }
            crate::relay::WebSocketCommand::CreateTerminal {
                thread_id,
                session_id,
                cols,
                rows,
                cwd,
                command,
            } => {
                let (resolved_thread_id, resolved_session_id, project_path) =
                    self.resolve_terminal_context(thread_id, session_id);
                let cwd = self.resolve_terminal_cwd(cwd, project_path)?;
                let info = self
                    .terminal_manager
                    .create_terminal(crate::terminal::TerminalSpec {
                        thread_id: resolved_thread_id,
                        session_id: resolved_session_id,
                        cwd,
                        command,
                        cols,
                        rows,
                    })
                    .await?;
                self.terminal_manager
                    .publish_snapshot(&info.terminal_id)
                    .await?;
            }
            crate::relay::WebSocketCommand::AttachTerminal {
                terminal_id,
                cols,
                rows,
            } => {
                self.terminal_manager
                    .attach_terminal(&terminal_id, cols, rows)
                    .await?;
            }
            crate::relay::WebSocketCommand::TerminalInput { terminal_id, data } => {
                self.terminal_manager
                    .write_input(&terminal_id, &data)
                    .await?;
            }
            crate::relay::WebSocketCommand::TerminalResize {
                terminal_id,
                cols,
                rows,
            } => {
                self.terminal_manager
                    .resize_terminal(&terminal_id, cols, rows)
                    .await?;
            }
            crate::relay::WebSocketCommand::CloseTerminal { terminal_id } => {
                self.terminal_manager.close_terminal(&terminal_id).await?;
            }
            crate::relay::WebSocketCommand::ListDirectories {
                thread_id,
                session_id,
                query,
            } => {
                let (resolved_tid, resolved_sid, project_path) = self.resolve_terminal_context(thread_id, session_id);
                let directories = self.list_directory_suggestions(&query, project_path.as_ref())?;
                self.session_event_sink
                    .publish(SessionEvent::DirectorySuggestions {
                        thread_id: resolved_tid,
                        acp_session_id: resolved_sid,
                        query,
                        directories,
                    })
                    .await;
            }
            crate::relay::WebSocketCommand::FindFiles {
                thread_id,
                session_id,
                query,
                start_directory,
            } => {
                let (resolved_tid, resolved_sid, project_path) = self.resolve_terminal_context(thread_id, session_id);
                let files = self.find_files(&query, project_path.as_ref(), start_directory)?;
                self.session_event_sink
                    .publish(SessionEvent::FindFilesResult {
                        thread_id: resolved_tid,
                        acp_session_id: resolved_sid,
                        query,
                        files,
                    })
                    .await;
            }
            crate::relay::WebSocketCommand::ReadFile {
                thread_id,
                session_id,
                path,
                start_line,
                line_count,
            } => {
                let (resolved_tid, resolved_sid, project_path) = self.resolve_terminal_context(thread_id, session_id);
                let (content, resolved_start, resolved_count, total_lines) = self.read_file(
                    &path,
                    start_line,
                    line_count,
                    project_path.as_ref(),
                )?;
                self.session_event_sink
                    .publish(SessionEvent::ReadFileResult {
                        thread_id: resolved_tid,
                        acp_session_id: resolved_sid,
                        path,
                        content,
                        start_line: resolved_start,
                        line_count: resolved_count,
                        total_lines,
                    })
                    .await;
            }
            crate::relay::WebSocketCommand::ListTerminals => {
                let snapshot = self.get_snapshot().await;
                for event in snapshot {
                    self.session_event_sink.publish(event).await;
                }
            }
            crate::relay::WebSocketCommand::ListSessions => {
                let snapshot = self.get_snapshot().await;
                for event in snapshot {
                    self.session_event_sink.publish(event).await;
                }
            }
        }
        Ok(())
    }
}
