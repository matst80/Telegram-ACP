use std::path::PathBuf;
use anyhow::Result;
use crate::daemon::DaemonHandle;

impl DaemonHandle {
    pub fn list_projects(&self) -> Vec<crate::relay::ProjectInfo> {
        let mut projects = Vec::new();
        if let Some(root) = &self.config.project_root {
            if let Ok(entries) = std::fs::read_dir(root) {
                for entry in entries.flatten() {
                    if let Ok(file_type) = entry.file_type() {
                        if file_type.is_dir() {
                            let path = entry.path();
                            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                                projects.push(crate::relay::ProjectInfo {
                                    name: name.to_string(),
                                    path,
                                });
                            }
                        }
                    }
                }
            }
        }
        projects.sort_by(|a, b| a.name.cmp(&b.name));
        projects
    }

    pub(crate) fn home_dir() -> Option<PathBuf> {
        std::env::var_os("HOME").map(PathBuf::from)
    }

    pub(crate) fn split_directory_query(query: &str) -> (String, String) {
        if query.ends_with('/') {
            return (query.to_string(), String::new());
        }

        match query.rsplit_once('/') {
            Some((base, fragment)) => (format!("{base}/"), fragment.to_string()),
            None => (String::new(), query.to_string()),
        }
    }

    pub(crate) fn resolve_directory_base(
        &self,
        raw_base: &str,
        project_path: Option<&PathBuf>,
    ) -> Result<PathBuf> {
        if raw_base.is_empty() {
            let resolved = project_path
                .cloned()
                .or_else(|| self.config.project_root.clone())
                .unwrap_or(std::env::current_dir()?);
            return Ok(resolved);
        }

        if raw_base == "~/" || raw_base.starts_with("~/") {
            let home = Self::home_dir().ok_or_else(|| anyhow::anyhow!("HOME is not set"))?;
            let relative = raw_base.trim_start_matches("~/");
            return Ok(home.join(relative));
        }

        let base_path = PathBuf::from(raw_base);
        if base_path.is_absolute() {
            return Ok(base_path);
        }

        let resolved = if let Some(project_path) = project_path {
            project_path.join(&base_path)
        } else {
            self.resolve_project_path(base_path)
        };
        Ok(resolved)
    }

    pub(crate) fn list_directory_suggestions(
        &self,
        query: &str,
        project_path: Option<&PathBuf>,
    ) -> Result<Vec<crate::relay::DirectorySuggestion>> {
        let (raw_base, fragment) = Self::split_directory_query(query.trim());
        let base_dir = self.resolve_directory_base(&raw_base, project_path)?;
        if !base_dir.is_dir() {
            return Ok(Vec::new());
        }

        let needle = fragment.to_lowercase();
        let mut directories = Vec::new();
        for entry in std::fs::read_dir(&base_dir)? {
            let entry = match entry {
                Ok(entry) => entry,
                Err(_) => continue,
            };
            let file_type = match entry.file_type() {
                Ok(file_type) => file_type,
                Err(_) => continue,
            };
            if !file_type.is_dir() {
                continue;
            }
            let name = match entry.file_name().into_string() {
                Ok(name) => name,
                Err(_) => continue,
            };
            if !needle.is_empty() && !name.to_lowercase().contains(&needle) {
                continue;
            }

            let path = format!("{}{name}/", raw_base);
            directories.push(crate::relay::DirectorySuggestion { path });
        }

        directories.sort_by(|a, b| a.path.cmp(&b.path));
        Ok(directories)
    }

    pub fn resolve_project_path(&self, path: PathBuf) -> PathBuf {
        if path.is_absolute() {
            return path;
        }

        if let Some(root) = &self.config.project_root {
            let joined = root.join(&path);
            if joined.exists() {
                return joined;
            }
        }
        path
    }

    pub(crate) fn resolve_terminal_context(
        &self,
        thread_id: Option<i32>,
        session_id: Option<String>,
    ) -> (Option<i32>, Option<String>, Option<PathBuf>) {
        let resolved_thread_id = self
            .session_manager
            .resolve_thread_id(thread_id, session_id.clone());
        let resolved_session_id = session_id.or_else(|| {
            resolved_thread_id
                .and_then(|tid| self.session_manager.get_acp_session_id_by_thread(tid))
        });
        let project_path = resolved_thread_id
            .and_then(|tid| self.session_manager.get_session_project_path_by_thread(tid));
        (resolved_thread_id, resolved_session_id, project_path)
    }

    pub(crate) fn resolve_terminal_cwd(
        &self,
        requested_cwd: Option<String>,
        project_path: Option<PathBuf>,
    ) -> Result<PathBuf> {
        let cwd = match requested_cwd {
            Some(cwd) => {
                let candidate = PathBuf::from(cwd);
                if candidate.is_absolute() {
                    candidate
                } else if let Some(project_path) = project_path {
                    project_path.join(candidate)
                } else {
                    self.resolve_project_path(candidate)
                }
            }
            None => project_path
                .or_else(|| self.config.project_root.clone())
                .unwrap_or(std::env::current_dir()?),
        };

        if !cwd.is_dir() {
            anyhow::bail!("Terminal cwd is not a directory: {}", cwd.display());
        }

        Ok(cwd)
    }

    pub(crate) fn find_files(
        &self,
        query: &str,
        project_path: Option<&PathBuf>,
        start_directory: Option<String>,
    ) -> Result<Vec<String>> {
        let base_dir = project_path
            .cloned()
            .or_else(|| self.config.project_root.clone())
            .unwrap_or(std::env::current_dir()?);

        if !base_dir.is_dir() {
            return Ok(Vec::new());
        }

        // 1. Gather all files in the project path
        let mut files = Vec::new();
        let mut rg_cmd = std::process::Command::new("rg");
        rg_cmd.arg("--files");
        if let Some(start_dir) = &start_directory {
            rg_cmd.arg(start_dir);
        }
        let rg_output = rg_cmd.current_dir(&base_dir).output();

        match rg_output {
            Ok(output) if output.status.success() => {
                let stdout_str = String::from_utf8_lossy(&output.stdout);
                for line in stdout_str.lines() {
                    if !line.is_empty() {
                        files.push(line.to_string());
                    }
                }
            }
            _ => {
                // Fallback to recursive walk if rg fails/not found
                tracing::warn!("rg --files failed or not found, falling back to manual traversal");
                
                // Parse gitignore rules
                let mut gitignore_patterns = Vec::new();
                if let Ok(content) = std::fs::read_to_string(base_dir.join(".gitignore")) {
                    for line in content.lines() {
                        let trimmed = line.trim();
                        if !trimmed.is_empty() && !trimmed.starts_with('#') {
                            gitignore_patterns.push(trimmed.to_string());
                        }
                    }
                }

                fn is_ignored(rel_str: &str, patterns: &[String]) -> bool {
                    for pattern in patterns {
                        let pat = pattern.trim_start_matches('/');
                        if pat.ends_with('/') {
                            let pat_dir = pat.trim_end_matches('/');
                            if rel_str.starts_with(pat_dir) || rel_str.contains(&format!("/{pat_dir}/")) {
                                return true;
                            }
                        } else {
                            if rel_str == pat || rel_str.starts_with(&format!("{pat}/")) || rel_str.ends_with(&format!("/{pat}")) || rel_str.contains(&format!("/{pat}/")) {
                                return true;
                            }
                        }
                    }
                    false
                }

                fn walk(
                    dir: &std::path::Path,
                    base: &std::path::Path,
                    patterns: &[String],
                    list: &mut Vec<String>,
                ) {
                    if let Ok(entries) = std::fs::read_dir(dir) {
                        for entry in entries.flatten() {
                            if let Ok(file_type) = entry.file_type() {
                                if let Ok(rel) = entry.path().strip_prefix(base) {
                                    let rel_str = rel.to_string_lossy().to_string();

                                    // Always ignore common large / system folders
                                    if let Some(name) = entry.file_name().to_str() {
                                        if name.starts_with('.') || name == "target" || name == "node_modules" {
                                            continue;
                                        }
                                    }

                                    if is_ignored(&rel_str, patterns) {
                                        continue;
                                    }

                                    if file_type.is_dir() {
                                        walk(&entry.path(), base, patterns, list);
                                    } else if file_type.is_file() {
                                        list.push(rel_str);
                                    }
                                }
                            }
                        }
                    }
                }

                let walk_dir = if let Some(start_dir) = start_directory {
                    let p = PathBuf::from(start_dir);
                    if p.is_absolute() {
                        p
                    } else {
                        base_dir.join(p)
                    }
                } else {
                    base_dir.clone()
                };

                walk(&walk_dir, &base_dir, &gitignore_patterns, &mut files);
            }
        }

        // 2. Score and filter by query
        if query.trim().is_empty() {
            // Sort by length, then alphabetically, limit to 50
            files.sort_by(|a, b| a.len().cmp(&b.len()).then_with(|| a.cmp(b)));
            files.truncate(50);
            return Ok(files);
        }

        let mut scored_files = Vec::new();
        for file in files {
            if let Some(score) = score_match(&file, query) {
                scored_files.push((score, file));
            }
        }

        // Sort descending by score, then alphabetically
        scored_files.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| a.1.cmp(&b.1)));

        let results = scored_files.into_iter().map(|(_, f)| f).take(50).collect();
        Ok(results)
    }

    pub(crate) fn read_file(
        &self,
        path: &str,
        start_line: Option<usize>,
        line_count: Option<usize>,
        project_path: Option<&PathBuf>,
    ) -> Result<(String, usize, usize, usize)> {
        let resolved_path = {
            let p = PathBuf::from(path);
            if p.is_absolute() {
                p
            } else if let Some(project_path) = project_path {
                project_path.join(p)
            } else {
                self.resolve_project_path(p)
            }
        };

        if !resolved_path.is_file() {
            anyhow::bail!("Path is not a file: {}", resolved_path.display());
        }

        let content = std::fs::read_to_string(&resolved_path)?;
        let lines: Vec<&str> = content.lines().collect();
        let total_lines = lines.len();

        let start = start_line.unwrap_or(1);
        let count = line_count.unwrap_or(400);

        if total_lines == 0 {
            return Ok((String::new(), start, count, 0));
        }

        let start_idx = if start > 0 { start - 1 } else { 0 };
        if start_idx >= total_lines {
            return Ok((String::new(), start, count, total_lines));
        }

        let end_idx = std::cmp::min(start_idx + count, total_lines);
        let sliced_content = lines[start_idx..end_idx].join("\n");

        Ok((sliced_content, start, count, total_lines))
    }
}

fn score_match(path: &str, query: &str) -> Option<i32> {
    let path_lower = path.to_lowercase();
    let query_lower = query.trim().to_lowercase();

    if query_lower.is_empty() {
        return Some(0);
    }

    // Check space-separated terms first
    let terms: Vec<&str> = query_lower.split_whitespace().collect();
    if terms.is_empty() {
        return Some(0);
    }

    // Check if path contains all terms as substrings
    let all_terms_substring = terms.iter().all(|term| path_lower.contains(term));
    if all_terms_substring {
        let matched_len: usize = terms.iter().map(|t| t.len()).sum();
        let score = 10000 + (matched_len as i32 * 10) - (path.len() as i32);
        return Some(score);
    }

    // Fall back to subsequence match (fuzzy) for the whole query (without spaces)
    let query_no_spaces: String = query_lower.chars().filter(|c| !c.is_whitespace()).collect();
    if query_no_spaces.is_empty() {
        return None;
    }

    let path_chars: Vec<char> = path_lower.chars().collect();
    let query_chars: Vec<char> = query_no_spaces.chars().collect();

    let mut path_idx = 0;
    let mut query_idx = 0;
    let mut score = 0;
    let mut last_match_idx = -1;

    while path_idx < path_chars.len() && query_idx < query_chars.len() {
        if path_chars[path_idx] == query_chars[query_idx] {
            if last_match_idx != -1 && path_idx as i32 == last_match_idx + 1 {
                score += 100; // Contiguous match bonus
            } else {
                score += 10;
            }
            last_match_idx = path_idx as i32;
            query_idx += 1;
        }
        path_idx += 1;
    }

    if query_idx == query_chars.len() {
        // Penalty for length of path to prefer shorter paths
        score -= path.len() as i32;
        Some(score)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use teloxide::Bot;
    use crate::terminal::TerminalManager;

    #[test]
    fn test_score_match() {
        // Contiguous matches should have high scores
        let score_exact = score_match("src/main.rs", "src/main.rs").unwrap();
        let score_substring = score_match("src/main.rs", "main").unwrap();
        let score_multi_term = score_match("src/main.rs", "src main").unwrap();
        let score_fuzzy = score_match("src/main.rs", "smn").unwrap();

        assert!(score_exact > score_substring);
        assert!(score_multi_term > score_fuzzy);

        // Subsequence mismatch
        assert!(score_match("src/main.rs", "xyz").is_none());
    }

    #[tokio::test]
    async fn test_read_file() {
        let config = crate::config::Config {
            bot_token: "test".to_string(),
            chat_id: 123,
            telegraph_author: None,
            telegraph_author_url: None,
            socket_path: std::path::PathBuf::from("/tmp/test.sock"),
            websocket_bind: None,
            default_agent: "test".to_string(),
            agents: std::collections::HashMap::new(),
            websocket_history_limit: 10,
            websocket_clipboard: false,
            global_clipboard_intercept: false,
            websocket_clipboard_poll_ms: 0,
            websocket_clipboard_max_bytes: 0,
            mcp_servers: std::collections::HashMap::new(),
            rag_register_url: None,
            rag_token: None,
            rag_register_name: None,
            rag_register_host: None,
            project_root: None,
        };
        let (local_start_tx, _) = tokio::sync::mpsc::unbounded_channel();
        let telegraph_client = telegraph_rs::Telegraph::new("dummy")
            .access_token("dummy_token")
            .create()
            .await
            .unwrap();

        let daemon = DaemonHandle {
            config,
            bot: Bot::new("token"),
            telegraph: std::sync::Arc::new(telegraph_client),
            session_event_sink: std::sync::Arc::new(crate::relay::NoopSessionEventSink),
            start_time: std::sync::atomic::AtomicI64::new(0),
            local_start_tx,
            session_manager: crate::session_manager::SessionManager::new(),
            terminal_manager: std::sync::Arc::new(TerminalManager::new(std::sync::Arc::new(crate::relay::NoopSessionEventSink))),
            pending_permissions: std::sync::Arc::new(dashmap::DashMap::new()),
            mdns: std::sync::Mutex::new(None),
        };

        let temp_dir = std::env::temp_dir();
        let temp_file = temp_dir.join("test_read.txt");
        std::fs::write(&temp_file, "line1\nline2\nline3\nline4").unwrap();

        let (content, start, count, total) = daemon.read_file(
            temp_file.to_str().unwrap(),
            Some(2),
            Some(2),
            None,
        ).unwrap();

        assert_eq!(content, "line2\nline3");
        assert_eq!(start, 2);
        assert_eq!(count, 2);
        assert_eq!(total, 4);

        std::fs::remove_file(temp_file).ok();
    }
}

