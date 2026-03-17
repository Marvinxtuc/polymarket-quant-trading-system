#!/bin/bash
set -euo pipefail

PLIST_SRC="/Users/marvin.xa/Desktop/Polymarket/scripts/ai.codex.poly.10h-thread-bridge.plist"
PLIST_DST="$HOME/Library/LaunchAgents/ai.codex.poly.10h-thread-bridge.plist"
RUNTIME_DIR="/tmp/poly_10h_thread_bridge"
LABEL="ai.codex.poly.10h-thread-bridge"
GUI_DOMAIN="gui/$(id -u)"

mkdir -p "$HOME/Library/LaunchAgents"
mkdir -p "$RUNTIME_DIR"

cp "/Users/marvin.xa/Desktop/Polymarket/scripts/start_10h_thread_bridge.sh" "$RUNTIME_DIR/start_10h_thread_bridge.sh"
cp "/Users/marvin.xa/Desktop/Polymarket/scripts/bridge_10h_thread_report.py" "$RUNTIME_DIR/bridge_10h_thread_report.py"
chmod +x "$RUNTIME_DIR/start_10h_thread_bridge.sh" "$RUNTIME_DIR/bridge_10h_thread_report.py"
cp "$PLIST_SRC" "$PLIST_DST"

launchctl bootout "$GUI_DOMAIN/$LABEL" >/dev/null 2>&1 || true
launchctl bootstrap "$GUI_DOMAIN" "$PLIST_DST"
launchctl kickstart -k "$GUI_DOMAIN/$LABEL"
