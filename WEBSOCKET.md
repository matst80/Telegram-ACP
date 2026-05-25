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
| `session_switched` | Active session in a topic was changed | Server -> Client |
| `session_ended` | Agent session terminated | Server -> Client |
| `session_removed` | Agent session and topic removed | Server -> Client |
| `send_prompt` | Send a command to the agent | Client -> Server |
| `cancel` | Interrupt current agent task | Client -> Server |

---

## Server -> Client Messages

### 1. `snapshot`
Sent immediately upon connection. Contains metadata and recent history for all active topics.

**Structure:**
```json
{
  "type": "snapshot",
  "sessions": [
    {
      "acp_session_id": "string",
      "project_path": "string",
      "status": "Initializing | Idle | Prompting | Finished | Error",
      "thread_id": "number",
      "agent_command": "string",
      "agent_name": "string | null",
      "history": [
        "Array of SessionEvent objects (see below)"
      ]
    }
  ]
}
```

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

### 4. `session_started` / `session_switched` / `session_ended` / `session_removed`
Broadcast when a session begins, is resumed, terminates, or is removed.

**Structure:**
```json
{
  "type": "session_started | session_switched | session_ended | session_removed",
  "thread_id": "number",
  "acp_session_id": "string | null",
  "folder": "string (for session_started/session_switched)",
  "name": "string | null (for session_started/session_switched)"
}
```

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
