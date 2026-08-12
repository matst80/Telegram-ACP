# Variables
RAG_TOKEN ?= rag_mcp_INf7UYdETxj9WgIDII6sCFoDq5UfWhG0NeKgL5iaHqg
RAG_URL ?= https://rag.k6n.net/api/acp/register
RAG_NAME ?= acp-mac
RAG_HOST ?= 10.10.10.205
TELEGRAM_ACP_WEBSOCKET_BIND ?= 0.0.0.0:9001
PORT ?= 9001
PROJECT_ROOT ?= /Users/mats/github.com/matst80

# Docker Variables
REGISTRY ?= registry.k6n.net
IMAGE_NAME ?= matst80/code-acp
IMAGE_TAG ?= latest
IMAGE ?= $(REGISTRY)/$(IMAGE_NAME):$(IMAGE_TAG)

.PHONY: help build build-terminal-share run-daemon run-terminal-share docker-build docker-push

help:
	@echo "Available targets:"
	@echo "  build                 Build the main project"
	@echo "  build-terminal-share  Build release binary for terminal-share"
	@echo "  run-daemon            Run the daemon with RAG registration and project root"
	@echo "  run-terminal-share    Run minimal standalone terminal share server"
	@echo "  docker-build          Build the docker image used for k8s deployment"
	@echo "  docker-push           Push the docker image to registry"

build:
	cargo build

build-terminal-share:
	cargo build --release --bin terminal_share

run-daemon:
	cargo run -- daemon \
		--websocket-bind 0.0.0.0:$(PORT) \
		--rag-register-url $(RAG_URL) \
		--rag-token $(RAG_TOKEN) \
		--project-root $(PROJECT_ROOT)

run-terminal-share:
	cargo run --bin terminal_share -- \
		--bind 0.0.0.0:$(PORT) \
		--rag-register-url $(RAG_URL) \
		--rag-token $(RAG_TOKEN) \
		--project-root $(PROJECT_ROOT)

docker-build:
	docker build -t $(IMAGE) .

docker-push:
	docker push $(IMAGE)
