#!/bin/bash
set -euo pipefail

PLIST_DST="$HOME/Library/LaunchAgents/ai.codex.poly.session-bridge.plist"
RUNTIME_DIR="/tmp/poly_codex_discord_bridge"
LABEL="ai.codex.poly.session-bridge"
GUI_DOMAIN="gui/$(id -u)"

launchctl bootout "$GUI_DOMAIN/$LABEL" >/dev/null 2>&1 || true
rm -f "$PLIST_DST"
rm -rf "$RUNTIME_DIR"
