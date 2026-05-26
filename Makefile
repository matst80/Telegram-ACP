# Variables
RAG_TOKEN ?= rag_mcp_INf7UYdETxj9WgIDII6sCFoDq5UfWhG0NeKgL5iaHqg
RAG_URL ?= https://rag.k6n.net/api/acp/register
RAG_NAME ?= acp-maxi
RAG_HOST ?= 10.10.10.205
TELEGRAM_ACP_WEBSOCKET_BIND ?= 0.0.0.0:9001
PORT ?= 9001
PROJECT_ROOT ?= /home/mats/github.com/matst80

.PHONY: help build run-daemon

help:
	@echo "Available targets:"
	@echo "  build         Build the project"
	@echo "  run-daemon    Run the daemon with RAG registration and project root"

build:
	cargo build

run-daemon:
	cargo run -- daemon \
		--websocket-bind 0.0.0.0:$(PORT) \
		--rag-register-url $(RAG_URL) \
		--rag-token $(RAG_TOKEN) \
		--rag-register-name $(RAG_NAME) \
		--project-root $(PROJECT_ROOT)
