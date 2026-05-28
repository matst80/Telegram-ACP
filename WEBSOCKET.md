# Telegram-ACP WebSocket API

The Telegram-ACP daemon provides a WebSocket server for real-time monitoring and control of agent sessions. It broadcasts session and terminal events, and it accepts JSON commands for session management, terminal I/O, and lightweight project browsing.

## Connection

- **Default Port:** `8000` (configurable via `websocket_bind`)
- **Protocol:** JSON over WebSocket
- **Discovery:** The service advertises itself via mDNS (Bonjour/Zeroconf) as `_acp-ws._tcp.local.`
- **Authentication:** Optional bearer-token auth. If `ACP_WS_TOKEN` or `TELEGRAM_ACP_WS_TOKEN` is set, clients must send `Authorization: Bearer <token>` or `?token=<token>` in the connection URL.

## Message Structure Overview

All websocket packets are JSON objects with a top-level `type` field.

| Message Type | Description | Direction |
| :--- | :--- | :--- |
| `snapshot` | Full current daemon/session snapshot | Server -> Client |
| `user_prompt` | User prompt recorded for a session | Server -> Client |
| `agent_update` | Agent activity, streamed content, tool calls, completion, errors | Server -> Client |
| `session_started` | A session became active in a topic | Server -> Client |
| `session_switched` | The active session in a topic changed | Server -> Client |
| `session_ended` | A session terminated | Server -> Client |
| `session_removed` | A headless session/topic entry was removed | Server -> Client |
| `permission_request` | ACP permission request awaiting a decision | Server -> Client |
| `telegram_thread_bound` | A session was bound to a Telegram topic | Server -> Client |
| `error` | Daemon-side validation or command error event | Server -> Client |
| `session_renamed` | Session/topic display name changed | Server -> Client |
| `topic_removed` | Topic entry removed from the daemon | Server -> Client |
| `clipboard_updated` | Local system clipboard changed | Server -> Client |
| `terminal_created` | A PTY-backed terminal was created | Server -> Client |
| `terminal_attached` | A client attached to an existing terminal | Server -> Client |
| `terminal_output` | Terminal emitted output bytes | Server -> Client |
| `terminal_snapshot` | Full terminal screen snapshot | Server -> Client |
| `terminal_resized` | Terminal dimensions changed | Server -> Client |
| `terminal_exited` | Terminal process exited | Server -> Client |
| `terminal_closed` | Terminal was removed from the daemon | Server -> Client |
| `terminal_error` | Terminal runtime error | Server -> Client |
| `directory_suggestions` | Directory typeahead suggestions | Server -> Client |
| `find_files_result` | Fuzzy file search results | Server -> Client |
| `read_file_result` | Slice of file contents | Server -> Client |
| `send_prompt` | Send a prompt to a session | Client -> Server |
| `cancel` | Interrupt current session work | Client -> Server |
| `set_config_option` | Change a session config option | Client -> Server |
| `set_permission_mode` | Change a session permission mode | Client -> Server |
| `spawn_session` | Start a new session (alias: `spawn`) | Client -> Server |
| `end_session` | End a session | Client -> Server |
| `permission_response` | Reply to a `permission_request` | Client -> Server |
| `bind_telegram_thread` | Bind a session to a Telegram topic | Client -> Server |
| `rename_session` | Rename a topic/session | Client -> Server |
| `remove_topic` | Remove a topic entry | Client -> Server |
| `execute_command` | Run an ACP available command | Client -> Server |
| `create_terminal` | Spawn a PTY-backed terminal | Client -> Server |
| `attach_terminal` | Attach to an existing terminal | Client -> Server |
| `terminal_input` | Send base64-encoded input bytes to a terminal | Client -> Server |
| `terminal_resize` | Resize a terminal | Client -> Server |
| `close_terminal` | Terminate and remove a terminal | Client -> Server |
| `list_directories` | Request directory suggestions | Client -> Server |
| `find_files` | Request fuzzy file search results | Client -> Server |
| `read_file` | Read a file slice | Client -> Server |
| `list_terminals` | Request a fresh snapshot of sessions and terminals | Client -> Server |
| `list_sessions` | Request a fresh snapshot of sessions and terminals | Client -> Server |

---

## Server -> Client Messages

### 1. `snapshot`
Sent immediately after the websocket connection is established. The same payload is also rebroadcast when the client sends `list_sessions` or `list_terminals`.

**Structure:**
```json
{
  "type": "snapshot",
  "rag_register_name": "string | null",
  "sessions": [
    {
      "acp_session_id": "string",
      "project_path": "string",
      "status": "Initializing | Idle | Prompting | Finished | Error",
      "thread_id": "number | null",
      "name": "string | null",
      "agent_command": "string",
      "agent_name": "string | null",
      "available_commands": [
        "Array of ACP AvailableCommand objects"
      ],
      "history": [
        {
          "type": "user_prompt",
          "thread_id": 123,
          "acp_session_id": "string | null",
          "text": "string",
          "content": [
            {
              "type": "text",
              "text": "string"
            }
          ]
        }
      ]
    }
  ],
  "projects": [
    {
      "name": "string",
      "path": "string"
    }
  ],
  "terminals": [
    {
      "terminal_id": "string",
      "thread_id": "number | null",
      "session_id": "string | null",
      "cwd": "string",
      "command": ["string", "..."],
      "cols": "number",
      "rows": "number",
      "running": "boolean",
      "exit_code": "number | null"
    }
  ]
}
```

**Notes:**
- `rag_register_name` is the daemon identity configured via `--rag-register-name` or `TELEGRAM_ACP_RAG_REGISTER_NAME`.
- `history` uses the same `SessionEvent` packet shapes that are broadcast live over the websocket.
- `available_commands`, `projects`, and `terminals` are omitted when empty.
- `agent_name` is the configured ACP alias for the session, for example `claude`, `codex`, or `copilot`.

### 2. `agent_update`
Broadcast when the agent emits activity for a session.

**Structure:**
```json
{
  "type": "agent_update",
  "thread_id": "number | null",
  "acp_session_id": "string",
  "event": {
    "type": "working | update | finished | error",
    "content": "string | { \"type\": \"text\", \"text\": \"string\" }",
    "sessionUpdate": "agent_message_chunk | agent_thought_chunk | tool_call | tool_call_update | plan | available_commands_update | usage_update",
    "title": "string",
    "toolCallId": "string",
    "status": "pending | in_progress | completed | failed",
    "entries": ["..."],
    "fields": {}
  }
}
```

For `event.type = "update"`, the ACP `SessionUpdate` payload is flattened directly onto `event`. There is no nested `event.update` object. For `finished` and `error`, `content` is a plain string.

**Examples:**

Agent working:
```json
{
  "type": "agent_update",
  "thread_id": 1,
  "acp_session_id": "session-1",
  "event": { "type": "working" }
}
```

Streaming text chunk:
```json
{
  "type": "agent_update",
  "thread_id": 1,
  "acp_session_id": "session-1",
  "event": {
    "type": "update",
    "sessionUpdate": "agent_message_chunk",
    "content": {
      "type": "text",
      "text": "The partial message..."
    }
  }
}
```

Tool call started:
```json
{
  "type": "agent_update",
  "thread_id": 1,
  "acp_session_id": "session-1",
  "event": {
    "type": "update",
    "sessionUpdate": "tool_call",
    "toolCallId": "uuid-123",
    "title": "Search Files",
    "kind": "search",
    "status": "in_progress"
  }
}
```

### 3. `user_prompt`
Broadcast when a user prompt is recorded for a session.

**Structure:**
```json
{
  "type": "user_prompt",
  "thread_id": "number | null",
  "acp_session_id": "string | null",
  "text": "string",
  "content": [
    {
      "type": "text",
      "text": "string"
    }
  ]
}
```

**Notes:**
- `content` is omitted when empty.
- For websocket-originated prompts, the daemon currently emits both `text` and a single text content block.

### 4. `session_started` and `session_switched`
Broadcast when a session becomes active in a topic or the active session changes.

**Structure:**
```json
{
  "type": "session_started | session_switched",
  "thread_id": "number | null",
  "acp_session_id": "string",
  "name": "string | null",
  "agent_name": "string | null",
  "agent_command": "string",
  "project_path": "string"
}
```

### 5. `session_ended` and `session_removed`
Broadcast when a session terminates or when a topic/session entry is removed.

**Structure:**
```json
{
  "type": "session_ended",
  "thread_id": "number | null",
  "acp_session_id": "string | null"
}
```

```json
{
  "type": "session_removed",
  "thread_id": "number"
}
```

### 6. `permission_request`
Broadcast when the ACP client asks the daemon to resolve a permission prompt.

**Structure:**
```json
{
  "type": "permission_request",
  "thread_id": "number | null",
  "acp_session_id": "string",
  "tool": "string",
  "args": {},
  "request_id": "string"
}
```

The client replies with `permission_response`.

### 7. `telegram_thread_bound`
Broadcast after binding a session to a Telegram topic, either by reusing an existing thread or by creating a new one.

**Structure:**
```json
{
  "type": "telegram_thread_bound",
  "session_id": "string",
  "thread_id": "number",
  "name": "string",
  "created": "boolean"
}
```

### 8. `error`
Broadcast for daemon-side validation or command handling errors that are intentionally surfaced as events.

**Structure:**
```json
{
  "type": "error",
  "in_reply_to": "string | null",
  "session_id": "string | null",
  "code": "string",
  "message": "string"
}
```

Examples include validation failures for `bind_telegram_thread`.

### 9. `session_renamed` and `topic_removed`

**Structure:**
```json
{
  "type": "session_renamed",
  "thread_id": "number",
  "acp_session_id": "string",
  "name": "string"
}
```

```json
{
  "type": "topic_removed",
  "thread_id": "number"
}
```

### 10. `clipboard_updated`
Broadcast when the daemon's optional clipboard watcher detects that the local system clipboard content has changed.

This event is daemon-scoped, not session-scoped.

**Structure:**
```json
{
  "type": "clipboard_updated",
  "source": "pbpaste | wl-paste | xclip | xsel",
  "content": "string",
  "truncated": "boolean"
}
```

**Notes:**
- Clipboard updates are broadcast live and are not stored in per-session history.
- The first observed clipboard value after the watcher starts is also emitted.

### 11. `terminal_created`
Broadcast after a new PTY-backed terminal has been created.

**Structure:**
```json
{
  "type": "terminal_created",
  "terminal": {
    "terminal_id": "string",
    "thread_id": "number | null",
    "session_id": "string | null",
    "cwd": "string",
    "command": ["string", "..."],
    "cols": "number",
    "rows": "number",
    "running": "boolean",
    "exit_code": "number | null"
  }
}
```

### 12. `terminal_attached`
Broadcast after a client attaches to an existing terminal.

**Structure:**
```json
{
  "type": "terminal_attached",
  "terminal_id": "string",
  "cols": "number",
  "rows": "number"
}
```

The daemon also emits a fresh `terminal_snapshot` immediately after attach succeeds.

### 13. `terminal_output`
Broadcast when terminal output arrives. `data` is base64-encoded raw output bytes.

**Structure:**
```json
{
  "type": "terminal_output",
  "terminal_id": "string",
  "sequence": "number",
  "data": "base64-string"
}
```

### 14. `terminal_snapshot`
Broadcast as a full-screen terminal snapshot. `data` is base64-encoded formatted screen state.

**Structure:**
```json
{
  "type": "terminal_snapshot",
  "terminal_id": "string",
  "sequence": "number",
  "cols": "number",
  "rows": "number",
  "cursor_row": "number",
  "cursor_col": "number",
  "data": "base64-string",
  "running": "boolean"
}
```

### 15. `terminal_resized`
Broadcast when a terminal is resized.

**Structure:**
```json
{
  "type": "terminal_resized",
  "terminal_id": "string",
  "cols": "number",
  "rows": "number"
}
```

### 16. `terminal_exited`, `terminal_closed`, and `terminal_error`

**Structure:**
```json
{
  "type": "terminal_exited",
  "terminal_id": "string",
  "exit_code": "number | null"
}
```

```json
{
  "type": "terminal_closed",
  "terminal_id": "string"
}
```

```json
{
  "type": "terminal_error",
  "terminal_id": "string | null",
  "message": "string"
}
```

**Behavior:**
- If a terminal is explicitly terminated via `close_terminal`, the daemon emits `terminal_closed`.
- If a terminal exits on its own, the daemon emits `terminal_exited` and then `terminal_closed`.
- `terminal_error` is emitted for PTY/runtime failures and may include a `terminal_id` when the failing terminal is known.

### 17. `directory_suggestions`
Broadcast in response to `list_directories`.

**Structure:**
```json
{
  "type": "directory_suggestions",
  "query": "string",
  "directories": [
    {
      "path": "string"
    }
  ]
}
```

**Behavior:**
- Only directories are returned.
- Suggestions preserve the caller's path style, for example `~/github.com/ma` can return `~/github.com/matst80/`.
- Relative paths are resolved against the associated project when `thread_id` or `session_id` is supplied.

### 18. `find_files_result`
Broadcast in response to `find_files`.

**Structure:**
```json
{
  "type": "find_files_result",
  "query": "string",
  "files": [
    "string"
  ]
}
```

**Behavior:**
- Up to 50 paths are returned, ordered by relevance.
- Matching is resolved relative to the associated project path.

### 19. `read_file_result`
Broadcast in response to `read_file`.

**Structure:**
```json
{
  "type": "read_file_result",
  "path": "string",
  "content": "string",
  "start_line": "number",
  "line_count": "number",
  "total_lines": "number"
}
```

**Fields:**
- `path`: The path that was read.
- `content`: The returned line slice.
- `start_line`: The 1-based start line of the returned slice.
- `line_count`: The requested or returned slice length.
- `total_lines`: Total line count in the file.

---

## Client -> Server Commands

Commands that act on an existing session usually accept `thread_id`, `session_id`, or both. When both are omitted, the daemon cannot resolve the target session.

### 1. `send_prompt`
```json
{
  "type": "send_prompt",
  "thread_id": "number | null",
  "session_id": "string | null",
  "text": "string"
}
```

The daemon records a matching `user_prompt` event before forwarding the prompt to the session.

### 2. `cancel`
```json
{
  "type": "cancel",
  "thread_id": "number | null",
  "session_id": "string | null"
}
```

### 3. `set_config_option`
Change an ACP session config option.

```json
{
  "type": "set_config_option",
  "thread_id": "number | null",
  "session_id": "string | null",
  "config_id": "string",
  "value_id": "string"
}
```

### 4. `set_permission_mode`
Change the ACP permission mode for a session.

```json
{
  "type": "set_permission_mode",
  "thread_id": "number | null",
  "session_id": "string | null",
  "mode_id": "string"
}
```

### 5. `spawn_session`
Start a new session. The daemon also accepts `spawn` as an alias for the packet `type`.

```json
{
  "type": "spawn_session",
  "project_path": "string",
  "agent_command": "string | null",
  "thread_id": "number | null"
}
```

**Notes:**
- `agent_command` also accepts the legacy field name `agent`.
- If `thread_id` is omitted, the daemon creates a headless session with an internal negative thread id.

### 6. `end_session`
```json
{
  "type": "end_session",
  "session_id": "string | null",
  "thread_id": "number | null"
}
```

If the session is headless, ending it can also emit `session_removed`.

### 7. `permission_response`
Reply to a pending `permission_request`.

```json
{
  "type": "permission_response",
  "request_id": "string",
  "decision": "string"
}
```

`decision` is sent back to ACP as the selected permission option id.

### 8. `bind_telegram_thread`
Bind an active session to a Telegram thread.

```json
{
  "type": "bind_telegram_thread",
  "session_id": "string",
  "thread_id": "number | null",
  "name": "string | null"
}
```

**Behavior:**
- If `thread_id` is a positive Telegram topic id, the session is rebound to that thread.
- If `thread_id` is omitted or non-positive, the daemon creates a new Telegram topic.
- When creating a topic, `name` is expected. If it is missing, the daemon currently falls back to a derived project name for migration compatibility.
- Validation failures are emitted as `error` events with `in_reply_to = "bind_telegram_thread"`.

### 9. `rename_session`
```json
{
  "type": "rename_session",
  "thread_id": "number | null",
  "session_id": "string | null",
  "name": "string"
}
```

On success the daemon emits `session_renamed` and, for real Telegram topics, also renames the forum topic remotely.

### 10. `remove_topic`
```json
{
  "type": "remove_topic",
  "thread_id": "number"
}
```

### 11. `execute_command`
Execute one ACP available command for a session.

```json
{
  "type": "execute_command",
  "thread_id": "number | null",
  "session_id": "string | null",
  "command_id": "string",
  "arguments": {}
}
```

### 12. `create_terminal`
Create a new PTY-backed terminal owned by the daemon.

```json
{
  "type": "create_terminal",
  "thread_id": "number | null",
  "session_id": "string | null",
  "cols": "number",
  "rows": "number",
  "cwd": "string | null",
  "command": ["string", "..."]
}
```

**Behavior:**
- `cols` and `rows` must be greater than `0`.
- If `cwd` is relative and a project context is available, it is resolved relative to that project path.
- If `cwd` is omitted, the daemon prefers the session project path, then `project_root`, then the daemon process working directory.
- On success the daemon emits `terminal_created` and then a `terminal_snapshot`.

### 13. `attach_terminal`
Attach to an existing terminal and set the caller's dimensions.

```json
{
  "type": "attach_terminal",
  "terminal_id": "string",
  "cols": "number",
  "rows": "number"
}
```

On success the daemon emits `terminal_attached` and then a fresh `terminal_snapshot`.

### 14. `terminal_input`
Send raw input bytes to a terminal.

```json
{
  "type": "terminal_input",
  "terminal_id": "string",
  "data": "base64-string"
}
```

`data` must be base64-encoded. This allows clients to send arbitrary terminal input bytes, including non-UTF-8 and control sequences.

### 15. `terminal_resize`
```json
{
  "type": "terminal_resize",
  "terminal_id": "string",
  "cols": "number",
  "rows": "number"
}
```

On success the daemon emits `terminal_resized`.

### 16. `close_terminal`
```json
{
  "type": "close_terminal",
  "terminal_id": "string"
}
```

### 17. `list_directories`
Request directory suggestions for path typeahead.

```json
{
  "type": "list_directories",
  "thread_id": "number | null",
  "session_id": "string | null",
  "query": "string"
}
```

### 18. `find_files`
Request fuzzy file search results in the current project directory.

```json
{
  "type": "find_files",
  "thread_id": "number | null",
  "session_id": "string | null",
  "query": "string"
}
```

### 19. `read_file`
Request file contents from the current project or an absolute path.

```json
{
  "type": "read_file",
  "thread_id": "number | null",
  "session_id": "string | null",
  "path": "string",
  "start_line": "number | null",
  "line_count": "number | null"
}
```

`start_line` defaults to `1`. `line_count` defaults to `400`.

### 20. `list_terminals`
```json
{
  "type": "list_terminals"
}
```

Currently this rebroadcasts the same `snapshot` packet shape used for initial connection state.

### 21. `list_sessions`
```json
{
  "type": "list_sessions"
}
```

Currently this also rebroadcasts the same `snapshot` packet shape used for initial connection state.

---

## Clipboard Relay Configuration

Clipboard relay is enabled by default whenever the websocket server is enabled.

Enable it with these config keys or environment variables:

- `websocket_clipboard` or `TELEGRAM_ACP_WEBSOCKET_CLIPBOARD`
- `global_clipboard_intercept` or `TELEGRAM_ACP_GLOBAL_CLIPBOARD_INTERCEPT`
- `websocket_clipboard_poll_ms` or `TELEGRAM_ACP_WEBSOCKET_CLIPBOARD_POLL_MS`
- `websocket_clipboard_max_bytes` or `TELEGRAM_ACP_WEBSOCKET_CLIPBOARD_MAX_BYTES`

Set `websocket_clipboard = false` or `TELEGRAM_ACP_WEBSOCKET_CLIPBOARD=false` to turn it off.

Global clipboard interception is a separate opt-in. On macOS it defaults to off, so websocket clipboard relay will not start the global clipboard listener unless `--global-clipboard-intercept`, `global_clipboard_intercept = true`, or `TELEGRAM_ACP_GLOBAL_CLIPBOARD_INTERCEPT=true` is set.

Example:

```toml
websocket_bind = "0.0.0.0:9001"
websocket_clipboard = true
global_clipboard_intercept = true
websocket_clipboard_poll_ms = 750
websocket_clipboard_max_bytes = 4096
```

Security note: clipboard contents often contain secrets, tokens, or personal data. Only enable this on trusted machines and trusted websocket networks.

---

## Constants Reference

### Session Status
- `Initializing`: Agent process starting up.
- `Idle`: Ready for a new prompt.
- `Prompting`: Currently processing a user prompt.
- `Finished`: Session closed normally.
- `Error`: Session crashed or failed to start.

### Tool Kind
- `read`, `edit`, `delete`, `move`, `search`, `execute`, `think`, `fetch`, `other`

### Tool Status
- `pending`, `in_progress`, `completed`, `failed`

### Plan Entry Status
- `pending`, `in_progress`, `completed`, `failed`, `cancelled`