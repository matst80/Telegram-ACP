# Base image for graph-cms development
ARG OS_IMAGE=mcr.microsoft.com/devcontainers/base:trixie
FROM ${OS_IMAGE}

# Prevent interactive prompts during package installation
ENV DEBIAN_FRONTEND=noninteractive

# Install system dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    curl \
    build-essential \
    git \
    fzf tmux ripgrep fd-find bat \
    rsync \
    jq \
    # Networking & System Info
    iputils-ping dnsutils net-tools iproute2 procps btop \
    # Text Editing & Viewing
    vim less tree \
    tmux \
    zsh \
    openssh-client \
    unzip \
    && rm -rf /var/lib/apt/lists/*

# Install Node.js LTS
RUN curl -fsSL https://deb.nodesource.com/setup_lts.x | bash - \
    && apt-get install -y nodejs \
    && rm -rf /var/lib/apt/lists/*

# Install Go
RUN GO_ARCH=$(dpkg --print-architecture) && \
    curl -fsSL "https://go.dev/dl/go1.26.3.linux-${GO_ARCH}.tar.gz" -o go.tar.gz && \
    tar -C /usr/local -xzf go.tar.gz && \
    rm go.tar.gz
ENV PATH="/usr/local/go/bin:/root/go/bin:${PATH}"

# Install Rustup
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"

# Install kubectl
RUN curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/$(dpkg --print-architecture)/kubectl" \
    && chmod +x kubectl \
    && mv kubectl /usr/local/bin/

# Install k9s
RUN K9S_VERSION=$(curl -sL -w "%{url_effective}" -o /dev/null https://github.com/derailed/k9s/releases/latest | awk -F/ '{print $NF}') \
    && curl -LO "https://github.com/derailed/k9s/releases/download/${K9S_VERSION}/k9s_Linux_$(dpkg --print-architecture).tar.gz" \
    && tar -xzf "k9s_Linux_$(dpkg --print-architecture).tar.gz" -C /usr/local/bin k9s \
    && rm "k9s_Linux_$(dpkg --print-architecture).tar.gz"

# Install GitHub CLI
RUN type -p curl >/dev/null || (apt-get update && apt-get install curl -y) \
    && curl -fsSL https://cli.github.com/packages/githubcli-archive-keyring.gpg | dd of=/usr/share/keyrings/githubcli-archive-keyring.gpg \
    && chmod go+r /usr/share/keyrings/githubcli-archive-keyring.gpg \
    && echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" | tee /etc/apt/sources.list.d/github-cli.list > /dev/null \
    && apt-get update \
    && apt-get install gh -y

# Install Docker CLI
RUN install -m 0755 -d /etc/apt/keyrings \
    && curl -fsSL https://download.docker.com/linux/debian/gpg -o /etc/apt/keyrings/docker.asc \
    && chmod a+r /etc/apt/keyrings/docker.asc \
    && echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/debian $(. /etc/os-release && echo "$VERSION_CODENAME") stable" \
    | tee /etc/apt/sources.list.d/docker.list > /dev/null \
    && apt-get update && apt-get install -y docker-ce-cli

# Install Delve (Go debugger)
RUN go install github.com/go-delve/delve/cmd/dlv@latest

# Install Jules and Gemini CLIs
RUN npm install -g @google/jules @google/gemini-cli --unsafe-perm

# Install Opencode
RUN curl -fsSL https://opencode.ai/install | bash

RUN curl -fsSL https://gh.io/copilot-install | bash

RUN curl -fsSL https://claude.ai/install.sh | bash

# Setup directories
#RUN mkdir -p /data/db /data/static-site /data/templates /data/schemas /root/.ssh && chmod 700 /root/.ssh


#RUN rm -rf /root/.oh-my-zsh
#RUN sh -c "$(curl -fsSL https://raw.githubusercontent.com/ohmyzsh/ohmyzsh/master/tools/install.sh)"

# Install VS Code CLI (includes `code tunnel`) and put it on PATH
RUN curl -Lk 'https://code.visualstudio.com/sha/download?build=stable&os=cli-alpine-x64' --output vscode_cli.tar.gz \
    && tar -xf vscode_cli.tar.gz -C /usr/local/bin \
    && rm vscode_cli.tar.gz


