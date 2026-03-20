#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
export PATH="/Users/marvin.xa/.nvm/versions/node/v24.5.0/bin:/usr/local/bin:/opt/homebrew/bin:/usr/bin:/bin:/usr/sbin:/sbin:$PATH"

exec /usr/bin/python3 "$SCRIPT_DIR/bridge_codex_session_reports_to_discord.py" \
  --sessions-dir "/Users/marvin.xa/.openclaw/agents/polymarket/sessions" \
  --codex-index "/Users/marvin.xa/.codex/session_index.jsonl" \
  --codex-sessions-root "/Users/marvin.xa/.codex/sessions" \
  --state-file "/tmp/poly_codex_discord_bridge/state.json" \
  --workspace "/Users/marvin.xa/Desktop/Polymarket" \
  --channel "discord" \
  --target "channel:1483402853648302081" \
  --account "default" \
  --openclaw-bin "/Users/marvin.xa/.nvm/versions/node/v24.5.0/bin/openclaw"
