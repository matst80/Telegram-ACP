FROM rust:1.95-bookworm AS builder

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        pkg-config \
        libssl-dev \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY tests ./tests
COPY README.md WEBSOCKET.md LICENSE ./

RUN cargo build --release --bin telegram-acp


FROM matst80/code-base:latest

ENV HOME=/root \
    TELEGRAM_ACP_CONFIG_DIR=/root/.config/telegram-acp \
    TELEGRAM_ACP_CONFIG_FILE=/root/.config/telegram-acp/config.toml \
    TELEGRAM_ACP_SOCKET_PATH=/tmp/telegram-acp.sock \
    TELEGRAM_ACP_WEBSOCKET_BIND=0.0.0.0:9001 \
    TELEGRAM_ACP_PROJECT_ROOT=/workspace

WORKDIR /workspace

COPY --from=builder /app/target/release/telegram-acp /usr/local/bin/telegram-acp
COPY docker/entrypoint.sh /usr/local/bin/telegram-acp-entrypoint

RUN mkdir -p /root/.config/telegram-acp /workspace \
    && chmod +x /usr/local/bin/telegram-acp-entrypoint

EXPOSE 9001 5900

CMD ["/usr/local/bin/telegram-acp-entrypoint", "telegram-acp", "daemon","--rag-register-url","https://rag.k6n.net/api/acp/register"]
