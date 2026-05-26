#!/usr/bin/env sh
set -eu

CONFIG_DIR="${TELEGRAM_ACP_CONFIG_DIR:-$HOME/.config/telegram-acp}"
CONFIG_FILE="${TELEGRAM_ACP_CONFIG_FILE:-$CONFIG_DIR/config.toml}"

mkdir -p "$CONFIG_DIR"

write_generated_config() {
    : "${TELEGRAM_ACP_BOT_TOKEN:?TELEGRAM_ACP_BOT_TOKEN is required when no config file is mounted}"
    : "${TELEGRAM_ACP_CHAT_ID:?TELEGRAM_ACP_CHAT_ID is required when no config file is mounted}"

    AGENT_NAME="${TELEGRAM_ACP_DEFAULT_AGENT:-codex}"
    AGENT_CMD="${TELEGRAM_ACP_DEFAULT_AGENT_CMD:-}"
    if [ -z "$AGENT_CMD" ]; then
        echo "TELEGRAM_ACP_DEFAULT_AGENT_CMD is required when no config file is mounted" >&2
        exit 1
    fi

    cat >"$CONFIG_FILE" <<EOF
bot_token = "${TELEGRAM_ACP_BOT_TOKEN}"
chat_id = ${TELEGRAM_ACP_CHAT_ID}
default_agent = "${AGENT_NAME}"
socket_path = "${TELEGRAM_ACP_SOCKET_PATH:-/tmp/telegram-acp.sock}"
websocket_bind = "${TELEGRAM_ACP_WEBSOCKET_BIND:-0.0.0.0:9001}"
EOF

    if [ -n "${TELEGRAM_ACP_TELEGRAPH_AUTHOR:-}" ]; then
        printf 'telegraph_author = "%s"\n' "$TELEGRAM_ACP_TELEGRAPH_AUTHOR" >>"$CONFIG_FILE"
    fi

    if [ -n "${TELEGRAM_ACP_PROJECT_ROOT:-}" ]; then
        printf 'project_root = "%s"\n' "$TELEGRAM_ACP_PROJECT_ROOT" >>"$CONFIG_FILE"
    fi

    cat >>"$CONFIG_FILE" <<EOF

[$AGENT_NAME]
cmd = "${AGENT_CMD}"
EOF

    if [ -n "${TELEGRAM_ACP_EXTRA_CONFIG:-}" ]; then
        printf '\n%s\n' "$TELEGRAM_ACP_EXTRA_CONFIG" >>"$CONFIG_FILE"
    fi
}

if [ ! -f "$CONFIG_FILE" ]; then
    write_generated_config
fi

if [ "${1:-}" = "telegram-acp" ] && [ "${2:-}" = "daemon" ]; then
    if command -v code >/dev/null 2>&1; then
        echo "Starting VS Code tunnel..."
        export VSCODE_CLI_USE_FILE_KEYCHAIN=1
        if [ -n "${VSCODE_TUNNEL_NAME:-}" ]; then
            code tunnel --accept-server-license-terms --name "${VSCODE_TUNNEL_NAME}" --no-sleep &
        else
            code tunnel --accept-server-license-terms --no-sleep &
        fi
    fi

    set -- "$@" \
        --websocket-bind "${TELEGRAM_ACP_WEBSOCKET_BIND:-0.0.0.0:9001}"

    if [ -n "${TELEGRAM_ACP_PROJECT_ROOT:-}" ]; then
        set -- "$@" --project-root "${TELEGRAM_ACP_PROJECT_ROOT}"
    fi
    if [ -n "${TELEGRAM_ACP_RAG_REGISTER_URL:-}" ]; then
        set -- "$@" --rag-register-url "${TELEGRAM_ACP_RAG_REGISTER_URL}"
    fi
    if [ -n "${TELEGRAM_ACP_RAG_TOKEN:-}" ]; then
        set -- "$@" --rag-token "${TELEGRAM_ACP_RAG_TOKEN}"
    fi
    if [ -n "${TELEGRAM_ACP_RAG_REGISTER_NAME:-}" ]; then
        set -- "$@" --rag-register-name "${TELEGRAM_ACP_RAG_REGISTER_NAME}"
    fi
    if [ -n "${TELEGRAM_ACP_RAG_REGISTER_HOST:-}" ]; then
        set -- "$@" --rag-register-host "${TELEGRAM_ACP_RAG_REGISTER_HOST}"
    fi
fi

exec "$@"
