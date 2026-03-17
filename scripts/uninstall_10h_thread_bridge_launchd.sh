#!/bin/bash
set -euo pipefail

PLIST_DST="$HOME/Library/LaunchAgents/ai.codex.poly.10h-thread-bridge.plist"
RUNTIME_DIR="/tmp/poly_10h_thread_bridge"
LABEL="ai.codex.poly.10h-thread-bridge"
GUI_DOMAIN="gui/$(id -u)"

launchctl bootout "$GUI_DOMAIN/$LABEL" >/dev/null 2>&1 || true
rm -f "$PLIST_DST"
rm -rf "$RUNTIME_DIR"
