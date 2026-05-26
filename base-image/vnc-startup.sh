#!/bin/bash
set -e

# Cleanup any existing locks
rm -f /tmp/.X0-lock /tmp/.X11-unix/X0
if pgrep -f 'chromium|chrome' >/dev/null; then
  echo "Chromium appears to be running; not removing Singleton files."
else
  rm -f ~/.config/chromium/Singleton*
fi
# Start Xvfb (virtual framebuffer)
Xvfb :0 -screen 0 1280x1024x24 &
sleep 1

# Start Window Manager
export DISPLAY=:0
# Launch Xfce session using dbus-launch to ensure session bus is available
dbus-launch --exit-with-session startxfce4 &
sleep 2
# Start clipboard manager (daemon) to improve clipboard sync and history
xfce4-clipman --daemon &

# Start VNC server (no password for dev environment)
x11vnc -display :0 -forever -nopw -listen localhost -xkb &
sleep 1

# Start noVNC (websockify)
# novnc is usually installed in /usr/share/novnc in Debian
echo "Remote Desktop is starting..."
/usr/share/novnc/utils/novnc_proxy --vnc localhost:5900 --listen 6080
