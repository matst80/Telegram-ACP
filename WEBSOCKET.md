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
      "acp_session_id": "string | null",
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

### 4. `session_started`
Broadcast when a session becomes active in a topic.

**Structure:**
```json
{
  "type": "session_started",
  "thread_id": "number | null",
  "acp_session_id": "string",
  "name": "string | null",
  "agent_name": "string | null",
  "agent_command": "string",
  "project_path": "string",
  "focus": "boolean"
}
```

- `focus`: If `true`, the client should consider switching UI focus to this session (e.g. it was initiated by a `/switch` command).

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
  "thread_id": "number",
  "acp_session_id": "string | null"
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

### 7. `telegram_thread_bound`
Broadcast after binding a session to a Telegram topic.

**Structure:**
```json
{
  "type": "telegram_thread_bound",
  "acp_session_id": "string",
  "thread_id": "number",
  "name": "string",
  "created": "boolean"
}
```

### 8. `error`
Broadcast for daemon-side validation or command handling errors.

**Structure:**
```json
{
  "type": "error",
  "in_reply_to": "string | null",
  "acp_session_id": "string | null",
  "thread_id": "number | null",
  "code": "string",
  "message": "string"
}
```

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

**Structure:**
```json
{
  "type": "clipboard_updated",
  "source": "pbpaste | wl-paste | xclip | xsel",
  "content": "string",
  "truncated": "boolean"
}
```

### 11. `terminal_created`
Broadcast after a new PTY-backed terminal has been created.

**Structure:**
```json
{
  "type": "terminal_created",
  "terminal": {
    "terminal_id": "string",
    "thread_id": "number | null",
    "acp_session_id": "string | null",
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
  "thread_id": "number | null",
  "acp_session_id": "string | null",
  "cols": "number",
  "rows": "number"
}
```

### 13. `terminal_output`
Broadcast when terminal output arrives.

**Structure:**
```json
{
  "type": "terminal_output",
  "terminal_id": "string",
  "thread_id": "number | null",
  "acp_session_id": "string | null",
  "sequence": "number",
  "data": "base64-string"
}
```

### 14. `terminal_snapshot`
Broadcast as a full-screen terminal snapshot.

**Structure:**
```json
{
  "type": "terminal_snapshot",
  "terminal_id": "string",
  "thread_id": "number | null",
  "acp_session_id": "string | null",
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
  "thread_id": "number | null",
  "acp_session_id": "string | null",
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
  "thread_id": "number | null",
  "acp_session_id": "string | null",
  "exit_code": "number | null"
}
```

```json
{
  "type": "terminal_closed",
  "terminal_id": "string",
  "thread_id": "number | null",
  "acp_session_id": "string | null"
}
```

```json
{
  "type": "terminal_error",
  "terminal_id": "string | null",
  "thread_id": "number | null",
  "acp_session_id": "string | null",
  "message": "string"
}
```

### 17. `directory_suggestions`
Broadcast in response to `list_directories`.

**Structure:**
```json
{
  "type": "directory_suggestions",
  "thread_id": "number | null",
  "acp_session_id": "string | null",
  "query": "string",
  "directories": [
    {
      "path": "string"
    }
  ]
}
```

### 18. `find_files_result`
Broadcast in response to `find_files`.

**Structure:**
```json
{
  "type": "find_files_result",
  "thread_id": "number | null",
  "acp_session_id": "string | null",
  "query": "string",
  "files": [
    "string"
  ]
}
```

### 19. `read_file_result`
Broadcast in response to `read_file`.

**Structure:**
```json
{
  "type": "read_file_result",
  "thread_id": "number | null",
  "acp_session_id": "string | null",
  "path": "string",
  "content": "string",
  "start_line": "number",
  "line_count": "number",
  "total_lines": "number"
}
```

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

### 2. `cancel`
```json
{
  "type": "cancel",
  "thread_id": "number | null",
  "session_id": "string | null"
}
```

### 3. `set_config_option`
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
```json
{
  "type": "set_permission_mode",
  "thread_id": "number | null",
  "session_id": "string | null",
  "mode_id": "string"
}
```

### 5. `spawn_session`
```json
{
  "type": "spawn_session",
  "project_path": "string",
  "agent_command": "string | null",
  "thread_id": "number | null"
}
```

### 6. `end_session`
```json
{
  "type": "end_session",
  "session_id": "string | null",
  "thread_id": "number | null"
}
```

### 7. `permission_response`
```json
{
  "type": "permission_response",
  "request_id": "string",
  "decision": "string"
}
```

### 8. `bind_telegram_thread`
```json
{
  "type": "bind_telegram_thread",
  "session_id": "string",
  "thread_id": "number | null",
  "name": "string | null"
}
```

### 9. `rename_session`
```json
{
  "type": "rename_session",
  "thread_id": "number | null",
  "session_id": "string | null",
  "name": "string"
}
```

### 10. `remove_topic`
```json
{
  "type": "remove_topic",
  "thread_id": "number"
}
```

### 11. `execute_command`
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

### 13. `attach_terminal`
```json
{
  "type": "attach_terminal",
  "terminal_id": "string",
  "cols": "number",
  "rows": "number"
}
```

### 14. `terminal_input`
```json
{
  "type": "terminal_input",
  "terminal_id": "string",
  "data": "base64-string"
}
```

### 15. `terminal_resize`
```json
{
  "type": "terminal_resize",
  "terminal_id": "string",
  "cols": "number",
  "rows": "number"
}
```

### 16. `close_terminal`
```json
{
  "type": "close_terminal",
  "terminal_id": "string"
}
```

### 17. `list_directories`
```json
{
  "type": "list_directories",
  "thread_id": "number | null",
  "session_id": "string | null",
  "query": "string"
}
```

### 18. `find_files`
```json
{
  "type": "find_files",
  "thread_id": "number | null",
  "session_id": "string | null",
  "query": "string",
  "start_directory": "string | null"
}
```

### 19. `read_file`
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

### 20. `list_terminals`
```json
{
  "type": "list_terminals"
}
```

### 21. `list_sessions`
```json
{
  "type": "list_sessions"
}
```

---

## Clipboard Relay Configuration

... (rest of the file)
