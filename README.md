# Telegram-ACP

**Control any coding agent through Telegram.**

| Screenshots | Screenshots | Screenshots |
|---|---|---|
| ![photo_1_2026-03-14_01-08-15](https://github.com/user-attachments/assets/e84143a7-58ac-4927-991f-b9cbb772aae2) | ![photo_2_2026-03-14_01-08-15](https://github.com/user-attachments/assets/18346bb9-39a9-4199-b206-e3fe7135e42b) | ![photo_2026-03-14_01-13-09](https://github.com/user-attachments/assets/53ac0332-0ca7-4683-bb36-f0d49e84f0a0) |
| **Talking with agent.** <br/> Handle multiple sessions with tabs and threads. | **Uploading artifacts.** <br/>Use telegraph to view your markdown files, or let it upload images or files. | **Slash commands.** <br/>Manage your sessions with ease, set models and permissions, or use agent's commands. |

## Features

- Get **notifications** and view **real-time progress** on your phone
- **Give new tasks** to agent even you're away from computer
- Works with **any agent** that supports [ACP](https://agentclientprotocol.com/). This is almost any agent, including claude code, codex, opencode and cursor.
- Handle **multiple sessions** simutaniously, in different telegram tabs

# Installation

**Method 1**: Use cargo binstall (not recommended)

`cargo binstall` requires me to manually bump version numbers and do a release, and there's no way to upload a binary on push.
Therefore, the version on `binstall` can be quite old.

```bash
cargo binstall telegram-acp
telegram-acp
```

**Method 2**: Use via nix

```bash
nix run github:SuperKenVery/Telegram-ACP
```

**Method 3**: Compile from source

```bash
git clone https://github.com/SuperKenVery/Telegram-ACP.git
cd Telegram-ACP
./dev-loop.fish
```

## Configuration

On Telegram, use `@botfather` to create a new bot and get your bot token. You should also **enable threaded mode in bot settings**.

Create `~/.config/telegram-acp/config.toml`:

```toml
bot_token = "<telegram-bot-token>"
chat_id = 123456789
default_agent = "claude"
# websocket_bind = "127.0.0.1:9001"

[claude]
cmd = "claude-agent-acp"

[codex]
cmd = "codex --acp"
# socket_path = "/tmp/telegram-acp.sock"
# telegraph_author = "Your Name"
```

Env overrides are also supported:

- `TELEGRAM_ACP_BOT_TOKEN`
- `TELEGRAM_ACP_CHAT_ID`
- `TELEGRAM_ACP_SOCKET_PATH`
- `TELEGRAM_ACP_WEBSOCKET_BIND`
- `TELEGRAM_ACP_DEFAULT_AGENT`
- `TELEGRAM_ACP_TELEGRAPH_AUTHOR`

When `websocket_bind` is set, the daemon starts a websocket listener and broadcasts each `SessionEvent` as a JSON text frame. The stream includes user prompts, agent updates, and session lifecycle events for every thread.

## Docker

A container image can be built from the included `Dockerfile`. It uses:

- a Rust builder stage to compile `telegram-acp`
- `matst80/graph-cms-base:latest` as the runtime image
- the runtime base for Chrome, coding tools, and `x11vnc`

Build it:

```bash
docker build -t telegram-acp:local .
```

Run it with a mounted config file:

```bash
docker run --rm \
  -p 9001:9001 \
  -p 5900:5900 \
  -v "$HOME/.config/telegram-acp:/root/.config/telegram-acp" \
  -v "$HOME/projects:/workspace" \
  telegram-acp:local
```

Or let the image generate a minimal config from env:

```bash
docker run --rm \
  -p 9001:9001 \
  -p 5900:5900 \
  -e TELEGRAM_ACP_BOT_TOKEN=... \
  -e TELEGRAM_ACP_CHAT_ID=123456789 \
  -e TELEGRAM_ACP_DEFAULT_AGENT=codex \
  -e TELEGRAM_ACP_DEFAULT_AGENT_CMD='codex --acp' \
  -e TELEGRAM_ACP_PROJECT_ROOT=/workspace \
  -v "$HOME/projects:/workspace" \
  telegram-acp:local
```

Useful environment variables for the container:

- `TELEGRAM_ACP_DEFAULT_AGENT_CMD` to define the agent command when auto-generating config
- `TELEGRAM_ACP_EXTRA_CONFIG` to append raw TOML to the generated config
- `TELEGRAM_ACP_WEBSOCKET_BIND` to change the websocket bind address
- `TELEGRAM_ACP_PROJECT_ROOT` to control the project directory exposed inside the container
- `TELEGRAM_ACP_RAG_REGISTER_URL`, `TELEGRAM_ACP_RAG_TOKEN`, `TELEGRAM_ACP_RAG_REGISTER_NAME`, `TELEGRAM_ACP_RAG_REGISTER_HOST` for RAG registration

# Hacking

## How it works

```text
CLI ──(Unix socket IPC)──> Daemon ──> ACP Agent subprocesses (stdin/stdout)
                              │
                              ├──> Telegram Bot API (topics, messages)
                              └──> Websocket listeners (session event stream)
```

Per session:

1. Creates/uses a Telegram forum topic (or threaded private chat topic)
2. Spawns an ACP agent subprocess
3. Routes user messages -> agent and agent events -> Telegram
4. Optionally broadcasts the same session activity to websocket listeners


## Mock agent testing

Use the included mock ACP binary to test Telegram/IPC plumbing without a real coding agent:

```sh
cargo run -- daemon
```

Configure it as an agent in your config:

```toml
default_agent = "mock"

[mock]
cmd = "./target/debug/mock_agent"
```

Then you can send some ACP updates as text via telegram, and it would send those updates to our daemon.

## Telegram requirements

- Enable **Threaded Mode** in BotFather
- For supergroups, forum topics should be enabled

## Some design decisions

- `agent-client-protocol` types are `!Send`, so ACP work is pinned to a `tokio::task::LocalSet`
- Telegram dispatcher + IPC server run with `tokio::spawn` and communicate through channels
- Each session has two unbounded channel pairs:
  - `user_tx`/`user_rx`: user text into prompt loop
  - `event_tx`/`event_rx`: agent output back to Telegram consumer
- A daemon-scoped `SessionEvent` relay fans out session activity to optional websocket listeners
- Notification behavior is intentional:
  - first and final message notify
  - intermediate streaming messages are silent
- Permission prompts are auto-approved by choosing the first allow option

## Project layout

```text
src/
  main.rs          CLI entrypoint (`daemon`, `new`, `status`)
  config.rs        Config loading (TOML + env overrides)
  daemon.rs        Daemon state, session lifecycle, LocalSet bridge
  session.rs       Prompt loop (PromptRequest orchestration)
  acp.rs           ACP client integration + subprocess handling
  relay.rs         SessionEvent model and fan-out sinks
  telegram.rs      Bot dispatcher, topic routing, event consumer
  telegraph.rs     Telegraph helpers (account/page publishing)
  ipc.rs           Unix socket NDJSON daemon/client protocol
  types.rs         Shared command/response/event types
  websocket.rs     Websocket broadcaster for SessionEvent listeners
  formatting.rs    Telegram HTML escaping + message splitting
  bin/mock_agent.rs Mock ACP agent for local testing
```

## License

GPL v3
