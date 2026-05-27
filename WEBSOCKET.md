# Telegram-ACP WebSocket API

The Telegram-ACP daemon provides a WebSocket server for real-time monitoring of agent sessions. It broadcasts events about user interactions and agent responses.

## Connection

- **Default Port:** 8000 (configurable via `websocket_bind`)
- **Protocol:** JSON over WebSocket
- **Discovery:** The service advertises itself via mDNS (Bonjour/Zeroconf) as `_acp-ws._tcp.local.`.

## Message Structure Overview

All messages sent by the server are JSON objects with a `type` field at the top level.

| Message Type | Description | Direction |
| :--- | :--- | :--- |
| `snapshot` | Initial state of all sessions | Server -> Client |
| `session_started` | A new session was created | Server -> Client |
| `user_prompt` | User sent a prompt to an agent | Server -> Client |
| `agent_update` | Agent activity (thinking, typing, tool calls) | Server -> Client |
| `clipboard_updated` | Local system clipboard changed | Server -> Client |
| `terminal_created` | A terminal was created | Server -> Client |
| `terminal_output` | Terminal emitted output bytes | Server -> Client |
| `terminal_snapshot` | Full terminal screen snapshot | Server -> Client |
| `terminal_exited` | Terminal process exited | Server -> Client |
| `terminal_closed` | Terminal was removed from the daemon | Server -> Client |
| `directory_suggestions` | Directory typeahead suggestions for path inputs | Server -> Client |
| `session_switched` | Active session in a topic was changed | Server -> Client |
| `session_ended` | Agent session terminated | Server -> Client |
| `session_removed` | Agent session and topic removed | Server -> Client |
| `send_prompt` | Send a command to the agent | Client -> Server |
| `cancel` | Interrupt current agent task | Client -> Server |
| `create_terminal` | Spawn a PTY-backed terminal process | Client -> Server |
| `close_terminal` | Terminate and remove a terminal | Client -> Server |
| `list_directories` | Request directory suggestions for typeahead | Client -> Server |

---

## Server -> Client Messages

### 1. `snapshot`
Sent immediately upon connection. Contains metadata and recent history for all active topics.

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
        "Array of SessionEvent objects (see below)"
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
- `projects` is included when `project_root` is configured and the daemon can enumerate child directories.
- `terminals` lists currently known PTY-backed terminals and is omitted when empty.
- `available_commands` is omitted when empty.
- `agent_name` is the configured ACP alias for the session, such as `claude`, `codex`, or `copilot`.

---

### 2. `agent_update`
Broadcast when the agent performs an action. This is the most complex and frequent message.

**Structure:**
```json
{
  "type": "agent_update",
  "thread_id": "number",
  "acp_session_id": "string",
  "event": {
    "type": "working | update | finished | error",
    "content": "string (only for finished/error)",
    "update": {
       "type": "agent_message_chunk | agent_thought_chunk | tool_call | tool_call_update | plan | available_commands_update | usage_update",
       "content": { "text": "string" },
       "title": "string (for tool_call)",
       "tool_call_id": "string",
       "status": "pending | in_progress | completed | failed",
       "entries": ["..."]
    }
  }
}
```

#### Detailed `agent_update.event` Variants:

**A. Agent is thinking:**
```json
{ "type": "agent_update", "thread_id": 1, "acp_session_id": "...", "event": { "type": "working" } }
```

**B. Streaming text chunk:**
```json
{
  "type": "agent_update",
  "thread_id": 1,
  "acp_session_id": "...",
  "event": {
    "type": "update",
    "session_update": {
      "type": "agent_message_chunk",
      "content": { "text": "The partial message..." }
    }
  }
}
```

**C. Tool Call started:**
```json
{
  "type": "agent_update",
  "thread_id": 1,
  "acp_session_id": "...",
  "event": {
    "type": "update",
    "session_update": {
      "type": "tool_call",
      "tool_call_id": "uuid-123",
      "title": "Search Files",
      "kind": "search",
      "status": "in_progress",
      "raw_input": "{\"query\": \"...\"}"
    }
  }
}
```

**D. Tool Call result (update):**
```json
{
  "type": "agent_update",
  "thread_id": 1,
  "acp_session_id": "...",
  "event": {
    "type": "update",
    "session_update": {
      "type": "tool_call_update",
      "tool_call_id": "uuid-123",
      "fields": {
        "status": "completed",
        "raw_output": "Search results..."
      }
    }
  }
}
```

---

### 3. `user_prompt`
Broadcast when a user sends a message via Telegram.

**Structure:**
```json
{
  "type": "user_prompt",
  "thread_id": "number",
  "acp_session_id": "string | null",
  "text": "string"
}
```

---

### 4. `clipboard_updated`
Broadcast when the daemon's optional clipboard watcher detects that the local system clipboard content has changed.

This event is daemon-scoped, not session-scoped: it does not include `thread_id` or `acp_session_id`.

**Structure:**
```json
{
  "type": "clipboard_updated",
  "source": "pbpaste | wl-paste | xclip | xsel",
  "content": "string",
  "truncated": "boolean"
}
```

**Fields:**
- `source`: The clipboard backend used on the host machine.
- `content`: Clipboard text content. This is emitted as UTF-8 text; non-UTF-8 bytes are lossy-decoded.
- `truncated`: `true` when the clipboard content exceeded the configured byte limit and was cut before sending.

**Notes:**
- This event is emitted whenever clipboard relay is enabled. Clipboard relay is on by default when the websocket server is enabled unless `websocket_clipboard` is set to `false`.
- Clipboard updates are not included in per-session history; they are broadcast live to connected websocket clients.
- The first observed clipboard value after the watcher starts is sent as a `clipboard_updated` event.

---

### 5. `session_started` / `session_switched` / `session_ended` / `session_removed`
Broadcast when a session begins, is resumed, terminates, or is removed.

**Structure:**
```json
{
  "type": "session_started | session_switched | session_ended | session_removed",
  "thread_id": "number",
  "acp_session_id": "string | null"
}
```

---

### 6. `terminal_created`
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

### 7. `terminal_output`
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

### 8. `terminal_snapshot`
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

### 9. `terminal_exited` / `terminal_closed`
Broadcast when a terminal process exits and when the daemon removes that terminal.

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

**Behavior:**
- If a terminal is explicitly terminated via `close_terminal`, the daemon kills it and emits `terminal_closed`.
- If a terminal exits or is killed outside the app, the daemon emits `terminal_exited` and then automatically removes it, followed by `terminal_closed`.
- After automatic removal, that terminal no longer appears in later `snapshot` payloads.

### 10. `directory_suggestions`
Broadcast in response to `list_directories`. This is intended for path input typeahead, for example resolving `~/` to directories under the caller's home directory.

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
- Suggestions preserve the caller's path style. For example, `~/github.com/ma` returns values like `~/github.com/matst80/`.
- Matching is case-insensitive and uses substring matching on the final path segment.
- Relative paths are resolved against the associated session project when `thread_id` or `session_id` is supplied.

---

## Client -> Server Commands

### 1. `send_prompt`
```json
{
  "type": "send_prompt",
  "thread_id": "number",
  "text": "string"
}
```

### 2. `cancel`
```json
{
  "type": "cancel",
  "thread_id": "number"
}
```

### 3. `create_terminal`
Create a new PTY-backed terminal owned by the daemon.

**Structure:**
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

**Fields:**
- `thread_id`: Optional Telegram thread id to associate with the terminal.
- `session_id`: Optional ACP session id to associate with the terminal. If `thread_id` is omitted, the daemon can use this to resolve the related session context.
- `cols`: Initial terminal width in columns. Must be greater than `0`.
- `rows`: Initial terminal height in rows. Must be greater than `0`.
- `cwd`: Optional working directory. If relative and a session/project context is available, it is resolved relative to that project path.
- `command`: Optional command argv vector. If omitted, the daemon starts the default shell for the host environment.

**Behavior:**
- If `cwd` is omitted, the daemon defaults to the associated project path when a session/thread is resolved.
- If there is no associated project path, it falls back to `project_root` when configured, otherwise the daemon's current working directory.
- If the resolved `cwd` does not exist or is not a directory, the command fails.
- On success, the server emits `terminal_created`, followed by `terminal_snapshot`, and then streams `terminal_output` events as data arrives.
- If the terminal later exits on its own, the server emits `terminal_exited` and then `terminal_closed`.

**Example:**
```json
{
  "type": "create_terminal",
  "thread_id": 123,
  "cols": 120,
  "rows": 36,
  "cwd": ".",
  "command": ["zsh"]
}
```

### 4. `close_terminal`
Terminate and remove an existing terminal.

**Structure:**
```json
{
  "type": "close_terminal",
  "terminal_id": "string"
}
```

### 5. `list_directories`
Request directory suggestions for path typeahead.

**Structure:**
```json
{
  "type": "list_directories",
  "thread_id": "number | null",
  "session_id": "string | null",
  "query": "string"
}
```

**Fields:**
- `thread_id`: Optional Telegram thread id used to resolve project-relative paths.
- `session_id`: Optional ACP session id used to resolve project-relative paths when `thread_id` is omitted.
- `query`: Partial path being completed. Examples: `~/`, `~/github.com/ma`, `.`, `src/han`.

**Behavior:**
- `~/` expands to the caller's home directory and returns all immediate child directories.
- Partial final path segments filter by substring match. For example, `~/github.com/ma` matches directories whose basename contains `ma`.
- Results are returned in a `directory_suggestions` event.

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
