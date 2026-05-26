use std::ffi::OsString;
use std::io::{Read, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU16, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use base64::Engine;
use dashmap::DashMap;
use portable_pty::{native_pty_system, CommandBuilder, PtySize};
use uuid::Uuid;

use crate::relay::{SessionEvent, SessionEventSink, TerminalInfo};

const DEFAULT_SCROLLBACK: usize = 10_000;

#[derive(Debug, Clone)]
pub struct TerminalSpec {
    pub thread_id: Option<i32>,
    pub session_id: Option<String>,
    pub cwd: PathBuf,
    pub command: Option<Vec<String>>,
    pub cols: u16,
    pub rows: u16,
}

pub struct TerminalManager {
    terminals: DashMap<String, Arc<TerminalHandle>>,
    event_sink: Arc<dyn SessionEventSink>,
    scrollback_limit: usize,
}

struct TerminalHandle {
    terminal_id: String,
    thread_id: Option<i32>,
    session_id: Option<String>,
    cwd: PathBuf,
    command: Vec<String>,
    cols: AtomicU16,
    rows: AtomicU16,
    running: AtomicBool,
    closed: AtomicBool,
    exit_code: Mutex<Option<i32>>,
    sequence: AtomicU64,
    master: Arc<Mutex<Box<dyn portable_pty::MasterPty + Send>>>,
    writer: Arc<Mutex<Box<dyn Write + Send>>>,
    killer: Arc<Mutex<Box<dyn portable_pty::ChildKiller + Send + Sync>>>,
    parser: Arc<Mutex<vt100::Parser>>,
}

enum InternalEvent {
    Output(Vec<u8>),
    Exited(Option<i32>),
    Error(String),
}

impl TerminalManager {
    pub fn new(event_sink: Arc<dyn SessionEventSink>) -> Self {
        Self {
            terminals: DashMap::new(),
            event_sink,
            scrollback_limit: DEFAULT_SCROLLBACK,
        }
    }

    pub async fn create_terminal(&self, spec: TerminalSpec) -> Result<TerminalInfo> {
        validate_size(spec.cols, spec.rows)?;
        if !spec.cwd.is_dir() {
            anyhow::bail!("Terminal cwd is not a directory: {}", spec.cwd.display());
        }

        let pty_system = native_pty_system();
        let size = pty_size(spec.cols, spec.rows);
        let pair = pty_system.openpty(size)?;
        let command = build_command(spec.command.clone(), &spec.cwd);
        let command_for_info = display_command(&command);
        let child = pair
            .slave
            .spawn_command(command)
            .context("Failed to spawn terminal process")?;
        let killer = child.clone_killer();
        let reader = pair.master.try_clone_reader()?;
        let writer = pair.master.take_writer()?;

        let terminal_id = Uuid::new_v4().to_string();
        let handle = Arc::new(TerminalHandle {
            terminal_id: terminal_id.clone(),
            thread_id: spec.thread_id,
            session_id: spec.session_id,
            cwd: spec.cwd,
            command: command_for_info,
            cols: AtomicU16::new(spec.cols),
            rows: AtomicU16::new(spec.rows),
            running: AtomicBool::new(true),
            closed: AtomicBool::new(false),
            exit_code: Mutex::new(None),
            sequence: AtomicU64::new(0),
            master: Arc::new(Mutex::new(pair.master)),
            writer: Arc::new(Mutex::new(writer)),
            killer: Arc::new(Mutex::new(killer)),
            parser: Arc::new(Mutex::new(vt100::Parser::new(
                spec.rows,
                spec.cols,
                self.scrollback_limit,
            ))),
        });

        self.terminals
            .insert(terminal_id.clone(), Arc::clone(&handle));
        self.spawn_output_tasks(Arc::clone(&handle), reader, child);

        self.event_sink
            .publish(SessionEvent::TerminalCreated {
                terminal: handle.info(),
            })
            .await;

        Ok(handle.info())
    }

    pub async fn attach_terminal(&self, terminal_id: &str, cols: u16, rows: u16) -> Result<()> {
        let handle = self
            .get_terminal(terminal_id)
            .with_context(|| format!("Unknown terminal: {terminal_id}"))?;
        handle.resize(cols, rows).await?;
        self.event_sink
            .publish(SessionEvent::TerminalAttached {
                terminal_id: terminal_id.to_string(),
                cols,
                rows,
            })
            .await;
        self.publish_snapshot(terminal_id).await
    }

    pub async fn write_input(&self, terminal_id: &str, data_b64: &str) -> Result<()> {
        let handle = self
            .get_terminal(terminal_id)
            .with_context(|| format!("Unknown terminal: {terminal_id}"))?;
        let data = base64::engine::general_purpose::STANDARD
            .decode(data_b64)
            .context("Invalid base64 terminal input")?;
        handle.write_input(data).await
    }

    pub async fn resize_terminal(&self, terminal_id: &str, cols: u16, rows: u16) -> Result<()> {
        let handle = self
            .get_terminal(terminal_id)
            .with_context(|| format!("Unknown terminal: {terminal_id}"))?;
        handle.resize(cols, rows).await?;
        self.event_sink
            .publish(SessionEvent::TerminalResized {
                terminal_id: terminal_id.to_string(),
                cols,
                rows,
            })
            .await;
        Ok(())
    }

    pub async fn close_terminal(&self, terminal_id: &str) -> Result<()> {
        let (_, handle) = self
            .terminals
            .remove(terminal_id)
            .with_context(|| format!("Unknown terminal: {terminal_id}"))?;
        handle.closed.store(true, Ordering::SeqCst);
        handle.kill().await?;
        self.event_sink
            .publish(SessionEvent::TerminalClosed {
                terminal_id: terminal_id.to_string(),
            })
            .await;
        Ok(())
    }

    pub async fn publish_snapshot(&self, terminal_id: &str) -> Result<()> {
        let handle = self
            .get_terminal(terminal_id)
            .with_context(|| format!("Unknown terminal: {terminal_id}"))?;
        self.event_sink.publish(handle.snapshot_event()).await;
        Ok(())
    }

    pub fn list(&self) -> Vec<TerminalInfo> {
        let mut terminals = self
            .terminals
            .iter()
            .map(|entry| entry.value().info())
            .collect::<Vec<_>>();
        terminals.sort_by(|a, b| a.terminal_id.cmp(&b.terminal_id));
        terminals
    }

    fn get_terminal(&self, terminal_id: &str) -> Option<Arc<TerminalHandle>> {
        self.terminals
            .get(terminal_id)
            .map(|entry| Arc::clone(entry.value()))
    }

    fn spawn_output_tasks(
        &self,
        handle: Arc<TerminalHandle>,
        mut reader: Box<dyn Read + Send>,
        mut child: Box<dyn portable_pty::Child + Send>,
    ) {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<InternalEvent>();
        let read_tx = tx.clone();
        std::thread::spawn(move || {
            let mut buf = [0_u8; 8192];
            loop {
                match reader.read(&mut buf) {
                    Ok(0) => break,
                    Ok(n) => {
                        if read_tx
                            .send(InternalEvent::Output(buf[..n].to_vec()))
                            .is_err()
                        {
                            break;
                        }
                    }
                    Err(err) => {
                        let _ = read_tx.send(InternalEvent::Error(err.to_string()));
                        break;
                    }
                }
            }
        });

        let wait_tx = tx.clone();
        std::thread::spawn(move || match child.wait() {
            Ok(status) => {
                let _ = wait_tx.send(InternalEvent::Exited(
                    i32::try_from(status.exit_code()).ok(),
                ));
            }
            Err(err) => {
                let _ = wait_tx.send(InternalEvent::Error(err.to_string()));
            }
        });

        let sink = Arc::clone(&self.event_sink);
        tokio::spawn(async move {
            while let Some(event) = rx.recv().await {
                if let Some(session_event) = handle.process_internal_event(event) {
                    sink.publish(session_event).await;
                }
            }
        });
    }
}

impl TerminalHandle {
    fn info(&self) -> TerminalInfo {
        TerminalInfo {
            terminal_id: self.terminal_id.clone(),
            thread_id: self.thread_id,
            session_id: self.session_id.clone(),
            cwd: self.cwd.clone(),
            command: self.command.clone(),
            cols: self.cols.load(Ordering::SeqCst),
            rows: self.rows.load(Ordering::SeqCst),
            running: self.running.load(Ordering::SeqCst),
            exit_code: *self.exit_code.lock().unwrap(),
        }
    }

    async fn write_input(&self, data: Vec<u8>) -> Result<()> {
        if self.closed.load(Ordering::SeqCst) {
            anyhow::bail!("Terminal is closed");
        }
        let writer = Arc::clone(&self.writer);
        tokio::task::spawn_blocking(move || -> Result<()> {
            let mut writer = writer.lock().unwrap();
            writer.write_all(&data)?;
            writer.flush()?;
            Ok(())
        })
        .await
        .context("Terminal input task failed")??;
        Ok(())
    }

    async fn resize(&self, cols: u16, rows: u16) -> Result<()> {
        validate_size(cols, rows)?;

        let master = Arc::clone(&self.master);
        tokio::task::spawn_blocking(move || -> Result<()> {
            master.lock().unwrap().resize(pty_size(cols, rows))?;
            Ok(())
        })
        .await
        .context("Terminal resize task failed")??;

        {
            let mut parser = self.parser.lock().unwrap();
            parser.set_size(rows, cols);
        }

        self.cols.store(cols, Ordering::SeqCst);
        self.rows.store(rows, Ordering::SeqCst);
        Ok(())
    }

    async fn kill(&self) -> Result<()> {
        let killer = Arc::clone(&self.killer);
        tokio::task::spawn_blocking(move || -> Result<()> {
            killer.lock().unwrap().kill()?;
            Ok(())
        })
        .await
        .context("Terminal kill task failed")??;
        Ok(())
    }

    fn process_internal_event(&self, event: InternalEvent) -> Option<SessionEvent> {
        if self.closed.load(Ordering::SeqCst) {
            return None;
        }

        match event {
            InternalEvent::Output(data) => {
                self.parser.lock().unwrap().process(&data);
                Some(SessionEvent::TerminalOutput {
                    terminal_id: self.terminal_id.clone(),
                    sequence: self.next_sequence(),
                    data: base64::engine::general_purpose::STANDARD.encode(data),
                })
            }
            InternalEvent::Exited(exit_code) => {
                self.running.store(false, Ordering::SeqCst);
                *self.exit_code.lock().unwrap() = exit_code;
                Some(SessionEvent::TerminalExited {
                    terminal_id: self.terminal_id.clone(),
                    exit_code,
                })
            }
            InternalEvent::Error(message) => Some(SessionEvent::TerminalError {
                terminal_id: Some(self.terminal_id.clone()),
                message,
            }),
        }
    }

    fn snapshot_event(&self) -> SessionEvent {
        let parser = self.parser.lock().unwrap();
        let screen = parser.screen();
        let (rows, cols) = screen.size();
        let (cursor_row, cursor_col) = screen.cursor_position();
        SessionEvent::TerminalSnapshot {
            terminal_id: self.terminal_id.clone(),
            sequence: self.sequence.load(Ordering::SeqCst),
            cols,
            rows,
            cursor_row,
            cursor_col,
            data: base64::engine::general_purpose::STANDARD.encode(screen.state_formatted()),
            running: self.running.load(Ordering::SeqCst),
        }
    }

    fn next_sequence(&self) -> u64 {
        self.sequence.fetch_add(1, Ordering::SeqCst) + 1
    }
}

fn validate_size(cols: u16, rows: u16) -> Result<()> {
    if cols == 0 || rows == 0 {
        anyhow::bail!("Terminal size must be greater than zero");
    }
    Ok(())
}

fn pty_size(cols: u16, rows: u16) -> PtySize {
    PtySize {
        rows,
        cols,
        pixel_width: 0,
        pixel_height: 0,
    }
}

fn build_command(command: Option<Vec<String>>, cwd: &PathBuf) -> CommandBuilder {
    let mut builder = if let Some(command) = command.filter(|cmd| !cmd.is_empty()) {
        CommandBuilder::from_argv(command.into_iter().map(OsString::from).collect())
    } else {
        CommandBuilder::new_default_prog()
    };
    builder.cwd(cwd.as_os_str());
    builder.env("TERM", "xterm-256color");
    builder.env("COLORTERM", "truecolor");
    builder
}

fn display_command(command: &CommandBuilder) -> Vec<String> {
    if command.is_default_prog() {
        vec!["$SHELL".to_string()]
    } else {
        command
            .get_argv()
            .iter()
            .map(|arg| arg.to_string_lossy().to_string())
            .collect()
    }
}
