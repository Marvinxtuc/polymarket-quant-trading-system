#!/bin/bash
set -euo pipefail

PLIST_SRC="/Users/marvin.xa/Desktop/Polymarket/scripts/ai.codex.poly.session-bridge.plist"
PLIST_DST="$HOME/Library/LaunchAgents/ai.codex.poly.session-bridge.plist"
RUNTIME_DIR="/tmp/poly_codex_discord_bridge"
LABEL="ai.codex.poly.session-bridge"
GUI_DOMAIN="gui/$(id -u)"

mkdir -p "$HOME/Library/LaunchAgents"
mkdir -p "$RUNTIME_DIR"

cp "/Users/marvin.xa/Desktop/Polymarket/scripts/start_codex_session_bridge.sh" "$RUNTIME_DIR/start_codex_session_bridge.sh"
cp "/Users/marvin.xa/Desktop/Polymarket/scripts/bridge_codex_session_reports_to_discord.py" "$RUNTIME_DIR/bridge_codex_session_reports_to_discord.py"
chmod +x "$RUNTIME_DIR/start_codex_session_bridge.sh" "$RUNTIME_DIR/bridge_codex_session_reports_to_discord.py"
cp "$PLIST_SRC" "$PLIST_DST"

/usr/bin/python3 "$RUNTIME_DIR/bridge_codex_session_reports_to_discord.py" --bootstrap-only

launchctl bootout "$GUI_DOMAIN/$LABEL" >/dev/null 2>&1 || true
launchctl bootstrap "$GUI_DOMAIN" "$PLIST_DST"
launchctl kickstart -k "$GUI_DOMAIN/$LABEL"
