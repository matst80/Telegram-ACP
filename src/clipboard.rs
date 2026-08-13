use std::ffi::OsString;
use std::path::PathBuf;
use std::time::Duration;

use tokio::process::Command;

use crate::relay::{SessionEvent, SessionEventSink};

#[derive(Clone, Copy, Debug)]
enum ClipboardBackend {
    PbPaste,
    WlPaste,
    Xclip,
    Xsel,
}

impl ClipboardBackend {
    fn detect() -> Option<Self> {
        if has_command("pbpaste") {
            return Some(Self::PbPaste);
        }
        if has_command("wl-paste") {
            return Some(Self::WlPaste);
        }
        if has_command("xclip") {
            return Some(Self::Xclip);
        }
        if has_command("xsel") {
            return Some(Self::Xsel);
        }
        None
    }

    fn source_name(self) -> &'static str {
        match self {
            Self::PbPaste => "pbpaste",
            Self::WlPaste => "wl-paste",
            Self::Xclip => "xclip",
            Self::Xsel => "xsel",
        }
    }

    async fn read(self) -> anyhow::Result<Vec<u8>> {
        let mut command = match self {
            Self::PbPaste => Command::new("pbpaste"),
            Self::WlPaste => {
                let mut command = Command::new("wl-paste");
                command.arg("--no-newline");
                command
            }
            Self::Xclip => {
                let mut command = Command::new("xclip");
                command.args(["-selection", "clipboard", "-o"]);
                command
            }
            Self::Xsel => {
                let mut command = Command::new("xsel");
                command.args(["--clipboard", "--output"]);
                command
            }
        };

        let output = command.output().await?;
        if output.status.success() {
            Ok(output.stdout)
        } else {
            anyhow::bail!(
                "{} exited with status {}",
                self.source_name(),
                output
                    .status
                    .code()
                    .map(|code| code.to_string())
                    .unwrap_or_else(|| "signal".to_string())
            );
        }
    }
}

pub fn spawn_watcher(
    event_sink: std::sync::Arc<dyn SessionEventSink>,
    poll_ms: u64,
    max_bytes: usize,
) {
    tokio::task::spawn_local(async move {
        let Some(backend) = ClipboardBackend::detect() else {
            tracing::warn!("Clipboard relay enabled but no supported clipboard reader was found");
            return;
        };

        tracing::info!(backend = backend.source_name(), poll_ms, max_bytes, "Clipboard relay started");

        let mut last_seen: Option<Vec<u8>> = None;
        let interval = Duration::from_millis(poll_ms.max(100));

        loop {
            match backend.read().await {
                Ok(content) => {
                    let changed = last_seen.as_ref().map(|previous| previous != &content).unwrap_or(true);
                    if changed {
                        let clipped_len = content.len().min(max_bytes.max(1));
                        let truncated = content.len() > clipped_len;
                        let payload = String::from_utf8_lossy(&content[..clipped_len]).into_owned();
                        event_sink
                            .publish(SessionEvent::ClipboardUpdated {
                                source: backend.source_name().to_string(),
                                content: payload,
                                truncated,
                            })
                            .await;
                        last_seen = Some(content);
                    }
                }
                Err(err) => {
                    tracing::debug!(backend = backend.source_name(), error = %err, "Clipboard read failed");
                }
            }

            tokio::time::sleep(interval).await;
        }
    });
}

fn has_command(name: &str) -> bool {
    let Some(paths) = std::env::var_os("PATH") else {
        return false;
    };

    std::env::split_paths(&paths).any(|dir| command_exists_in_dir(&dir, name))
}

fn command_exists_in_dir(dir: &PathBuf, name: &str) -> bool {
    let candidate = dir.join(name);
    if candidate.is_file() {
        return true;
    }

    #[cfg(windows)]
    {
        let exe_candidate = dir.join(OsString::from(format!("{name}.exe")));
        return exe_candidate.is_file();
    }

    #[cfg(not(windows))]
    {
        let _ = &OsString::new();
        false
    }
}