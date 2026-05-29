# Refactor findings for websocket fan-out

## Summary

The repo should be refactored around **one new relay module**, not a blanket conversion to traits. Right now user messages, agent updates, and topic/session lifecycle changes enter the system through different modules and become Telegram-specific too early:

- `telegram.rs` sends user prompts straight to `SessionCommand::Prompt`
- `acp.rs` turns ACP notifications into `AgentEvent` and pushes them into a per-session channel
- `daemon.rs` owns topic/session lifecycle and persistence
- `session.rs` consumes `AgentEvent` locally and emits Telegram output through `EventContext`
- `handlers/mod.rs` embeds Telegram transport details directly in the handler context

That shape makes websocket listeners possible, but only by hooking three separate places. The highest-leverage refactor is to introduce a **single seam for session activity** at daemon scope, then keep Telegram and websocket delivery as adapters behind it.

## Status after implementation

The main refactor is now **done** for the current websocket fan-out goal.

- Implemented `src/relay.rs` with `SessionEvent`, `SessionEventSink`, and composite fan-out.
- Routed user prompts, agent updates, and lifecycle events through the relay from `telegram.rs`, `acp.rs`, `session.rs`, and `daemon.rs`.
- Added websocket broadcasting in `src/websocket.rs`, enabled from daemon config via `websocket_bind` / `TELEGRAM_ACP_WEBSOCKET_BIND`.
- Kept Telegram rendering behavior unchanged.

The two remaining follow-ups from the original plan are now **resolved as decisions**:

1. **Do not add `McpActivity` yet.** MCP traffic remains outside the canonical `SessionEvent` seam because the current websocket goal is satisfied by thread/session activity, not raw protocol tracing.
2. **Do not extract `EventWriter` yet.** Websocket clients now receive canonical `SessionEvent` JSON directly, so there is no second adapter for rendered Telegram-style handler output.

For the current end goal, there are **no required follow-up refactors left**. The only remaining work would come from future product requirements.

## Current friction

### 1. Session activity is split across three modules

**Files**: `src/telegram.rs`, `src/acp.rs`, `src/daemon.rs`, `src/session.rs`

There is no module whose interface means “everything that happened in a session/thread.” Instead:

- user input is emitted in `handle_topic_message()`
- agent output is emitted in `TelegramClient::session_notification()`
- lifecycle updates live in `DaemonHandle::start_session_local()` and related topic/session code

This gives low **locality**: a websocket adapter would need to attach to multiple places at once.

### 2. Event consumption is deep, but its seam is local to Telegram delivery

**Files**: `src/session.rs`, `src/handlers/mod.rs`, `src/handlers/*.rs`

`run_event_consumer()` is not a shallow pass-through. It coordinates draft flushing, working indicators, tool call state, plan state, completion/error handling, and command caching. That is a real module with real **leverage**.

But its interface is effectively “consume events and send Telegram messages,” because `EventContext` hardcodes:

- `Bot`
- `ChatId`
- `thread_id`
- Telegram send/edit/delete/pin/close operations

So the implementation is deeper than the current seam.

### 3. Persistence exists, but it is not the right first seam

**Files**: `src/session_log.rs`, `src/persistence.rs`

`SessionLog` and `save_topics()` are useful modules, but they are not where websocket fan-out should start. They record session state and transcripts after the fact. If websocket propagation is added here first, it will be a second, parallel path rather than the main seam for live activity.

By the deletion test, these modules are not the core place where transport complexity currently concentrates.

## Recommended deepening opportunities

### 1. Introduce a `SessionEvent` relay module at daemon scope

**Files**: `src/relay.rs`, `src/daemon.rs`, `src/telegram.rs`, `src/acp.rs`, `src/session.rs`, `src/websocket.rs`

**Problem**

There is no unified interface for session/thread activity. The current implementation forces every new listener to know about the Telegram input path, the ACP output path, and daemon lifecycle updates separately.

**Solution**

Create a deep module around a single event stream:

```rust
pub enum SessionEvent {
    UserPrompt {
        thread_id: i32,
        acp_session_id: Option<String>,
        text: String,
    },
    AgentUpdate {
        thread_id: i32,
        acp_session_id: String,
        update: AgentEvent,
    },
    SessionStarted { thread_id: i32, acp_session_id: String },
    SessionSwitched { thread_id: i32, acp_session_id: String },
    SessionEnded { thread_id: i32 },
}

#[async_trait::async_trait]
pub trait SessionEventSink: Send + Sync {
    async fn publish(&self, event: SessionEvent);
}
```

Then publish into that seam from:

- `telegram.rs::handle_topic_message()`
- `acp.rs::TelegramClient::session_notification()`
- session start/switch/restore/remove code in `daemon.rs`

Where the identifiers are known, include both `thread_id` and `acp_session_id`. That keeps the interface deep for both Telegram-oriented and ACP-oriented consumers and avoids extra lookup logic in websocket listeners.

**Benefits**

- **Locality**: websocket fan-out logic moves to one place instead of three
- **Leverage**: one interface captures all thread activity, so new listeners reuse the same module
- **Testability**: an in-memory adapter can assert on the full session flow without spinning up Telegram

**Seam status**

This is now a **real seam**:

1. a Telegram-facing adapter that preserves today’s behavior
2. a websocket adapter that broadcasts to listeners

This was the most important refactor, and it is now implemented.

### 2. Split `EventContext` into handler state + transport adapter

**Files**: `src/handlers/mod.rs`, `src/handlers/*.rs`, `src/session.rs`

**Problem**

The handler module is already where event presentation state lives, but its interface is locked to Telegram transport details. That makes the module shallower than it should be because callers must know about the transport through the context.

**Solution**

Keep the handler implementation and state machines, but replace the Telegram-specific context with a writer seam:

```rust
#[async_trait::async_trait(?Send)]
pub trait EventWriter {
    async fn send_html(&mut self, text: &str, silent: bool) -> Option<OutputRef>;
    async fn edit_html(&mut self, id: OutputRef, text: &str) -> bool;
    async fn delete(&mut self, id: OutputRef);
    async fn pin(&mut self, id: OutputRef);
    async fn close_thread(&mut self);
}
```

`EventContext` can then own:

- transport-neutral throttling
- per-turn state shared by handlers
- `writer: Box<dyn EventWriter>`

Start with a `TelegramEventWriter` adapter. A websocket adapter may not need every operation, so a second step could define a separate projection writer for listener-facing output if needed.

**Benefits**

- **Locality**: Telegram delivery concerns stop leaking through every handler
- **Leverage**: the same handler module can drive multiple outputs
- **Testability**: handlers can be tested with a recording adapter instead of Telegram calls

**Seam status**

This was evaluated after websocket fan-out landed. The result is: **do not extract this seam now**. Websocket listeners consume canonical `SessionEvent` payloads rather than rendered Telegram output, so an `EventWriter` would still be a **hypothetical seam** with limited **leverage**.

### 3. Add a composite sink, not multiple direct hooks

**Files**: new relay module plus `src/daemon.rs`

**Problem**

Once websocket support exists, there will be pressure to call Telegram delivery, websocket broadcast, logging, and maybe metrics from each producer. That would spread fan-out logic across the codebase again.

**Solution**

Make the relay module own fan-out through a composite adapter:

```rust
pub struct MultiSessionEventSink {
    sinks: Vec<Arc<dyn SessionEventSink>>,
}
```

That keeps Telegram, websocket, and later metrics/webhook adapters behind one seam.

The sink should be held on `DaemonHandle` behind `Arc`, so new sessions and event publishers can use shared daemon state instead of threading sink ownership through each call path. That preserves **locality** at the same place where session lifecycle already lives.

**Benefits**

- **Locality**: producer modules publish once
- **Leverage**: new adapters do not require touching all producers
- **Testability**: tests can replace the composite with a single recording adapter

**Seam status**

This is a **real seam** as soon as Telegram + websocket both exist.

### 4. MCP traffic is an optional extension, not part of the first seam

**Files**: `src/mcp.rs`, `src/mcp_relay.rs`, `src/daemon.rs`

**Problem**

The repo has MCP traffic flowing through the daemon, but websocket listeners may not need raw protocol-level request and response payloads to satisfy the main goal of propagating messages and threads.

**Solution**

Keep the first seam focused on canonical session activity. Only extend `SessionEvent` with MCP-specific variants if websocket consumers explicitly need protocol tracing, for example:

```rust
pub enum McpDirection {
    Inbound,
    Outbound,
}

pub enum SessionEvent {
    // ...
    McpActivity {
        thread_id: i32,
        mcp_session_id: String,
        payload: String,
        direction: McpDirection,
    },
}
```

**Benefits**

- keeps the initial interface smaller and more focused
- avoids widening the seam before there is a second real consumer for that detail

**Seam status**

This decision is now closed for the current scope: keep MCP traffic out of `SessionEvent`. It remains a **conditional seam** that should only be added if websocket consumers explicitly need MCP visibility.

### 5. Consider a recorder seam only after live fan-out exists

**Files**: `src/session_log.rs`, `src/persistence.rs`, maybe new recorder module later

**Problem**

The file-backed recording modules are concrete implementations with only one adapter today.

**Solution**

Do **not** refactor them to traits yet. If later you need the same write path for:

- disk transcript recording
- webhook delivery
- durable websocket replay

then introduce an `EventRecorder` seam around recording only.

**Benefits**

- avoids a shallow module now
- preserves focus on the live transport seam first

**Seam status**

Right now this would be a **hypothetical seam**.

## Places where a trait would be premature

### `SessionCommand`

**Files**: `src/session_control.rs`, `src/session.rs`, `src/telegram.rs`, `src/commands/*.rs`

This is just the command language for the session runtime. A trait here would not add **depth**; it would just obscure a clear enum-based interface.

### IPC command handling

**Files**: `src/ipc.rs`, `src/daemon.rs`

Websocket listeners are about event propagation, not command transport. A shared command-handler trait could wait until websocket control commands actually exist.

### `SessionLog`

**Files**: `src/session_log.rs`

There is only one adapter today: file-backed recording. A trait now would be a hypothetical seam with little **leverage**.

## Recommended refactor order

1. **Done**: add `SessionEvent` + `SessionEventSink` as the main seam.
2. **Done**: publish all user/agent/lifecycle activity into that seam.
3. **Done**: implement a composite sink with websocket fan-out.
4. **Done**: decide that MCP traffic does **not** belong in the seam for the current goal.
5. **Done**: decide that handler rendering does **not** need its own `EventWriter` seam for the current goal.
6. **Still true**: leave persistence and IPC concrete until they gain a real second adapter.

## Future steps only if requirements change

These are no longer part of the current refactor plan, but they remain valid later extensions:

1. Add `McpActivity` to `SessionEvent` only if websocket consumers need protocol-level MCP tracing.
2. Extract an `EventWriter` seam only if another consumer needs the same rendered, Telegram-like handler output.
3. Revisit recorder/persistence seams only if replay, metrics, or webhook delivery needs a second concrete adapter.

## Testing shape for the new seam

The relay module should be easy to test without Telegram, ACP subprocesses, or websocket infrastructure. A simple in-memory sink is enough to prove the seam fans out correctly:

```rust
let test_event = SessionEvent::SessionStarted {
    thread_id: Some(42),
    acp_session_id: "test-session-id".to_string(),
    name: Some("project: swift river".to_string()),
    agent_name: Some("copilot".to_string()),
    agent_command: "gemini --acp".to_string(),
    project_path: std::path::PathBuf::from("/tmp/project"),
    focus: false,
};

composite.publish(test_event).await;

assert_eq!(list1.lock().await.len(), 1);
assert_eq!(list2.lock().await.len(), 1);
```

That test shape is a good signal that the module has real **leverage**: producers publish once, and multiple adapters observe the same event without involving Telegram-specific implementation details.

## Strong recommendation

If the end goal is “propagate all messages and threads to websocket listeners,” the best design is **not** “trait-ify everything.” The deep module to introduce is a **session activity relay** centered on a single `SessionEvent` interface. That seam gives the most **leverage** and **locality** because it matches the domain concept you actually want to expose: everything that happened in a thread/session.

The second refactor worth doing is the handler writer seam, but only if websocket consumers also need the rendered, Telegram-like output stream. If they only need canonical events, keep websocket fan-out above the handlers and avoid a shallow transport abstraction there.
