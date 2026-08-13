# Running `terminal_share` as a Systemd User Service

This guide explains how to install and autostart `terminal_share` as a Linux systemd user service.

---

## 1. Build the Release Binary

Run the Makefile target to build the optimized release binary:

```bash
make build-terminal-share
```

The binary will be located at `target/release/terminal_share`.

Optionally install it to `~/.local/bin` (or `/usr/local/bin`):

```bash
mkdir -p ~/.local/bin
cp target/release/terminal_share ~/.local/bin/
```

---

## 2. Create the Systemd Service File

Create the systemd user service unit directory if it doesn't exist:

```bash
mkdir -p ~/.config/systemd/user
```

Create `~/.config/systemd/user/terminal-share.service` with the following content:

```ini
[Unit]
Description=Terminal Share Server
After=network.target

[Service]
ExecStart=%h/.local/bin/terminal_share \
  --bind 0.0.0.0:9001 \
  --rag-register-url https://rag.k6n.net/api/acp/register \
  --rag-token YOUR_RAG_TOKEN \
  --rag-register-name acp-mac \
  --rag-register-host 10.10.10.205 \
  --project-root %h
Restart=on-failure
RestartSec=5s

[Install]
WantedBy=default.target
```

> **Note:**
> - `%h` is automatically expanded by systemd to your home directory (`/home/username`).
> - Adjust `--bind`, `--rag-token`, `--rag-register-url`, and `--project-root` as needed.

---

## 3. Enable and Start the Service

Reload the systemd user manager, enable the service to start at boot/login, and start it immediately:

```bash
# Reload systemd user configuration
systemctl --user daemon-reload

# Enable service to start automatically on login/boot
systemctl --user enable terminal-share.service

# Start the service immediately
systemctl --user start terminal-share.service
```

---

## 4. Useful Management Commands

- **Check status:**
  ```bash
  systemctl --user status terminal-share.service
  ```
- **View logs:**
  ```bash
  journalctl --user -u terminal-share.service -f
  ```
- **Stop service:**
  ```bash
  systemctl --user stop terminal-share.service
  ```
- **Restart service:**
  ```bash
  systemctl --user restart terminal-share.service
  ```

---

## 5. (Optional) Enable Linger for Headless Autostart

By default, user services start when you log in and stop when you log out. To allow user services to start on system boot and run even when you are logged out:

```bash
loginctl enable-linger $USER
```
