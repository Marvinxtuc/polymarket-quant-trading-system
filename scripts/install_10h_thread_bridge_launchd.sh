#!/bin/bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PLIST_SRC="${PROJECT_ROOT}/scripts/ai.codex.poly.10h-thread-bridge.plist"
PLIST_DST="$HOME/Library/LaunchAgents/ai.codex.poly.10h-thread-bridge.plist"
RUNTIME_DIR="/tmp/poly_10h_thread_bridge"
LABEL="ai.codex.poly.10h-thread-bridge"
GUI_DOMAIN="gui/$(id -u)"

mkdir -p "$HOME/Library/LaunchAgents"
mkdir -p "$RUNTIME_DIR"

cp "${PROJECT_ROOT}/scripts/start_10h_thread_bridge.sh" "$RUNTIME_DIR/start_10h_thread_bridge.sh"
cp "${PROJECT_ROOT}/scripts/bridge_10h_thread_report.py" "$RUNTIME_DIR/bridge_10h_thread_report.py"
chmod +x "$RUNTIME_DIR/start_10h_thread_bridge.sh" "$RUNTIME_DIR/bridge_10h_thread_report.py"
cp "$PLIST_SRC" "$PLIST_DST"

launchctl bootout "$GUI_DOMAIN/$LABEL" >/dev/null 2>&1 || true
launchctl bootstrap "$GUI_DOMAIN" "$PLIST_DST"
launchctl kickstart -k "$GUI_DOMAIN/$LABEL"
