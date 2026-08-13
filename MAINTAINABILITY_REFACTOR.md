# Maintainability Refactor Assessment

## Scope

This review focuses on maintainability risks caused by oversized modules and responsibility overlap, with SOLID principles as the main lens.

The biggest risks are not simple line count problems. The real issue is that several modules combine orchestration, transport details, persistence, and protocol handling in the same place. That makes changes expensive, increases the number of call sites that must be touched together, and makes local testing harder than it needs to be.

## Executive Summary

Highest-priority refactor targets:

1. `src/daemon/session_starter.rs` - session startup and resume flow is too broad and owns too many side effects.
2. `src/session.rs` - runtime command loop mixes queueing, ACP command handling, status transitions, logging, and event publication.
3. `src/relay.rs` - the websocket/event contract has become a cross-domain dumping ground.
4. `src/handlers/mod.rs` - event consumer orchestration, Telegram writing, throttling, and handler composition live in one module.
5. `src/acp.rs` - ACP client behavior is tightly coupled to Telegram delivery and permission UX.
6. `src/daemon/command_handler.rs` - a large websocket command dispatcher mixes unrelated command families.

Lower-priority but worthwhile:

- `src/daemon/mod.rs` should be thinned into a smaller composition root.
- `src/session_manager.rs` should stop combining lookup helpers with persistence.

Large files that look relatively cohesive and should not be first-wave refactor targets:

- `src/formatting.rs`
- `src/session_log.rs`
- likely `src/terminal.rs` (based on ownership boundary and file role, though this review did not do a full deep read there)

## Largest Files Reviewed

Approximate current sizes:

| File | Lines | Observation |
| --- | ---: | --- |
| `src/daemon/session_starter.rs` | 791 | High-risk lifecycle god module |
| `src/relay.rs` | 757 | High-risk protocol/event aggregation |
| `src/handlers/mod.rs` | 704 | High-risk transport + orchestration mix |
| `src/session.rs` | 532 | High-risk runtime state machine in one file |
| `src/acp.rs` | 531 | High-risk ACP + Telegram coupling |
| `src/daemon/command_handler.rs` | 494 | High-risk command dispatch sprawl |
| `src/formatting.rs` | 488 | Large, but mostly cohesive utility code |
| `src/daemon/directory_service.rs` | 483 | Not deeply reviewed here |
| `src/mcp.rs` | 427 | Not deeply reviewed here |
| `src/terminal.rs` | 414 | Appears domain-focused from current architecture |

## High-Priority Refactor Targets

### 1. `src/daemon/session_starter.rs`

**Why it is a maintainability problem**

This file currently owns almost the entire lifecycle of creating or restoring a session:

- topic creation and replacement
- start request enqueueing
- history sink setup
- session log creation
- MCP session creation
- event consumer spawning
- session entry construction
- ACP init/resume wiring
- persistence updates
- lifecycle event publication
- child-process cleanup

The main evidence is `start_session_local()`, followed by `spawn_and_run_agent()` and `init_agent()`. Those three functions form one long chain of orchestration with many dependencies and many side effects.

**SOLID pressure**

- **Single Responsibility Principle**: session creation, runtime wiring, persistence mutation, and transport-specific behavior are all in the same module.
- **Dependency Inversion Principle**: high-level lifecycle code depends directly on concrete Telegram, ACP, MCP, persistence, and sink implementations.
- **Open/Closed Principle**: adding a new startup concern means touching the same orchestration path again.

**Recommended split**

Split this module by lifecycle phase rather than by helper-function size:

- `daemon/session_bootstrap.rs`: build channels, logs, history sink, session entry, and shared state.
- `daemon/agent_launcher.rs`: spawn ACP process, initialize or resume session, own stderr/IO handling.
- `daemon/session_registry.rs`: install/remove active session entries and update topic history.
- `daemon/session_restoration.rs`: restore existing sessions and replay persisted state.

**Practical target shape**

Introduce a small `SessionStartContext` struct and make each phase consume or enrich it. That removes the current pattern of pushing 10+ parameters through multiple async functions.

**Priority**

Very high. This is the best single refactor for reducing future change cost.

### 2. `src/session.rs`

**Why it is a maintainability problem**

`run_session_runtime()` is doing too much at once. It handles:

- prompt queueing
- command dispatch
- cancellation
- permission mode changes
- config option changes
- custom command execution
- status transitions
- event publication
- transcript logging

`run_event_consumer()` is smaller, but it is still coupled to Telegram-facing consumer flow and command cache updates.

**SOLID pressure**

- **Single Responsibility Principle**: runtime coordination, ACP command translation, state mutation, and event emission are fused together.
- **Open/Closed Principle**: each new `SessionCommand` branch makes the main select loop broader.
- **Dependency Inversion Principle**: runtime logic depends directly on concrete ACP connection types and concrete event channels.

**Recommended split**

- `session/runtime.rs`: own the select loop only.
- `session/prompt_queue.rs`: queueing and prompt activation rules.
- `session/command_dispatch.rs`: map `SessionCommand` variants to ACP operations.
- `session/status.rs`: explicit status transitions and lifecycle rules.
- `session/event_publish.rs`: publish `AgentEvent` and `SessionEvent` consistently.

**Practical target shape**

Create a `SessionRuntime` struct with a small internal API such as:

- `handle_command(...)`
- `handle_cancel(...)`
- `handle_prompt_completion(...)`
- `start_next_prompt_if_needed(...)`

The first goal is not abstraction for its own sake. The goal is to stop `tokio::select!` branches from owning business logic directly.

**Priority**

Very high. This is the main runtime hotspot.

### 3. `src/relay.rs`

**Why it is a maintainability problem**

This file currently mixes:

- cross-cutting event contracts
- websocket command contracts
- session snapshot types
- sink traits
- concrete sink implementations
- serialization tests

The `SessionEvent` enum now spans many domains: session lifecycle, agent updates, Telegram binding, terminals, clipboard, directory search, file reads, and snapshots. `WebSocketCommand` has the same problem on the input side.

This is not just a long enum. It means the core relay boundary is no longer modeling one domain cleanly.

**SOLID pressure**

- **Single Responsibility Principle**: protocol contracts, sink implementations, and tests are in one place.
- **Interface Segregation Principle**: consumers that only care about session lifecycle must still depend on a type that also carries terminal, clipboard, and file-reading concerns.
- **Open/Closed Principle**: every new websocket feature expands the same giant contract file.

**Recommended split**

- `relay/events/session.rs`
- `relay/events/terminal.rs`
- `relay/events/workspace.rs`
- `relay/commands/session.rs`
- `relay/commands/terminal.rs`
- `relay/commands/workspace.rs`
- `relay/sinks.rs`

Keep a thin `relay/mod.rs` that re-exports stable public types.

**Practical target shape**

If the websocket API must remain flat externally, keep the serialized schema flat but move internal enum definitions behind domain modules. The external wire shape does not require the internal code to stay centralized.

**Priority**

Very high. This file is becoming the protocol junk drawer.

### 4. `src/handlers/mod.rs`

**Why it is a maintainability problem**

This module currently contains:

- throttling logic
- output reference types
- `EventWriter` abstraction
- `TelegramEventWriter` implementation
- `EventContext`
- `EventHandler` trait
- `SessionEventConsumer`
- tests

The submodules under `handlers/` are a good direction, but the parent module is still carrying too much infrastructure and transport behavior.

**SOLID pressure**

- **Single Responsibility Principle**: transport policy, write adapter, consumer composition, and handler contracts are mixed.
- **Dependency Inversion Principle**: the abstraction is present, but the module still centers the Telegram implementation instead of keeping transport code at the edge.

**Recommended split**

- `handlers/throttle.rs`
- `handlers/writer.rs`
- `handlers/telegram_writer.rs`
- `handlers/context.rs`
- `handlers/consumer.rs`

Keep `handlers/mod.rs` as a small export surface only.

**Priority**

High. Not as risky as `session.rs` or `session_starter.rs`, but it is already past the point where navigation stays cheap.

### 5. `src/acp.rs`

**Why it is a maintainability problem**

`TelegramClient` is not just an ACP client adapter. It also owns:

- Telegram bot/message details
- permission UI behavior
- relay event publication
- session transcript logging
- notification fan-out

That means a protocol adapter is also acting as a Telegram-facing application service.

**SOLID pressure**

- **Single Responsibility Principle**: ACP protocol integration and Telegram UX are coupled.
- **Dependency Inversion Principle**: high-level protocol code depends directly on `Bot`, `ChatId`, inline keyboards, and concrete sink types.

**Recommended split**

- `acp/client.rs`: ACP-facing client adapter only.
- `acp/permission_presenter.rs`: permission approval flow and Telegram UI.
- `acp/event_forwarder.rs`: translate ACP notifications into internal events.
- `acp/process.rs`: process spawn and stderr/IO support.

Rename `TelegramClient` after extraction so the name reflects its real role. Right now the type name hides how much application logic it owns.

**Priority**

High. This will matter more as soon as there is another UI or API surface besides Telegram.

### 6. `src/daemon/command_handler.rs`

**Why it is a maintainability problem**

The websocket command handler has one broad match over unrelated command families:

- prompt/session commands
- permission/config commands
- session creation and teardown
- Telegram thread binding
- terminal commands
- directory and file utilities

That is a classic sign that the input surface has grown faster than the internal command model.

**SOLID pressure**

- **Single Responsibility Principle**: one module coordinates unrelated feature families.
- **Open/Closed Principle**: adding commands keeps extending the same match and the same dependency set.

**Recommended split**

- `daemon/commands/session_commands.rs`
- `daemon/commands/telegram_binding.rs`
- `daemon/commands/terminal_commands.rs`
- `daemon/commands/workspace_commands.rs`

Then keep one thin dispatcher that routes by family.

**Priority**

High. It is already large enough that unrelated changes will keep colliding here.

## Medium-Priority Refactor Targets

### 7. `src/daemon/mod.rs`

This file is functioning as both composition root and application service surface. It creates the daemon, configures websocket fan-out, starts IPC, starts websocket services, manages mDNS, and exposes lifecycle helpers on `DaemonHandle`.

That is acceptable at smaller scale, but it is turning `DaemonHandle` into a broad dependency bucket.

**Recommended direction**

- keep `daemon/mod.rs` as composition root only
- move runtime services behind smaller fields such as `session_service`, `websocket_service`, and `discovery_service`

This is important, but it should follow the first-wave splits above.

### 8. `src/session_manager.rs`

This file is not very large, but it is strategically important. It contains a growing list of lookup helpers and also persists topics to disk.

Current issues:

- many getters expose internal storage structure directly
- thread/session resolution logic is duplicated across helper methods
- persistence is mixed into the in-memory state container

**SOLID pressure**

- **Single Responsibility Principle**: in-memory registry plus persistence.
- **Interface Segregation Principle**: callers depend on many narrowly tailored getter methods.

**Recommended direction**

- extract persistence into `session_repository` or `session_persistence`
- introduce a smaller query-oriented API for resolving active session state

This is a good supporting refactor, but it should not be the first place to start.

## Lower-Priority Large Files

### `src/formatting.rs`

This file is large, but it mostly looks like cohesive formatting and truncation utilities. It has a size problem more than an architecture problem.

If it becomes painful to navigate, split by output kind:

- message formatting
- tool formatting
- truncation/splitting helpers

This is a readability refactor, not an urgent SOLID repair.

### `src/session_log.rs`

This appears cohesive around transcript and session logging. It is not a priority refactor target from a SOLID standpoint.

### `src/types.rs`

This file is small and cohesive. It should not be treated as a refactor target in the same wave as the large orchestration files.

## Recommended Refactor Sequence

### Phase 1: reduce the biggest orchestration hotspots

1. Split `src/daemon/session_starter.rs` by lifecycle phase.
2. Extract `SessionRuntime` internals out of `src/session.rs`.
3. Split `src/handlers/mod.rs` into consumer, writer, and throttle modules.

### Phase 2: reduce protocol and command sprawl

1. Split `src/relay.rs` into domain-specific events, commands, and sinks.
2. Split `src/daemon/command_handler.rs` by command family.

### Phase 3: harden boundaries

1. Decouple ACP protocol handling from Telegram UX in `src/acp.rs`.
2. Thin `src/daemon/mod.rs` into a composition root plus smaller services.
3. Move persistence out of `src/session_manager.rs`.

## Ground Rules For The Refactor

To avoid creating abstraction noise, the refactor should follow a few constraints:

- split by responsibility, not by arbitrary line count
- prefer structs with explicit dependencies over free functions with long argument lists
- keep Telegram-specific code at the edge
- keep wire-format compatibility for websocket consumers while changing internal module layout
- add narrow unit tests around extracted components before moving more logic

## Final Recommendation

If only one area can be tackled first, start with `src/daemon/session_starter.rs` and `src/session.rs` together. Those two files currently define most of the session lifecycle and runtime behavior, and they carry the highest change risk.

If the goal is the fastest maintainability win with lower behavior risk, start with `src/handlers/mod.rs` and `src/relay.rs`. Those refactors improve navigation and dependency boundaries without changing the core ACP runtime as aggressively.