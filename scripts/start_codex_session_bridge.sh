#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CODEX_HOME="${CODEX_HOME:-$HOME/.codex}"
OPENCLAW_HOME="${OPENCLAW_HOME:-$HOME/.openclaw}"
POLYMARKET_WORKSPACE="${POLYMARKET_WORKSPACE:-$(cd "${SCRIPT_DIR}/.." && pwd)}"
OPENCLAW_NODE_BIN_DIR="${OPENCLAW_NODE_BIN_DIR:-$HOME/.nvm/versions/node/v24.5.0/bin}"
OPENCLAW_BIN="${OPENCLAW_BIN:-$OPENCLAW_NODE_BIN_DIR/openclaw}"
SYSTEM_PATH_DEFAULT="/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"
if command -v brew >/dev/null 2>&1; then
  SYSTEM_PATH_DEFAULT="$(brew --prefix)/bin:${SYSTEM_PATH_DEFAULT}"
fi
export PATH="${OPENCLAW_NODE_BIN_DIR}:${POLY_CLI_PATH:-$SYSTEM_PATH_DEFAULT}:$PATH"

exec /usr/bin/python3 "$SCRIPT_DIR/bridge_codex_session_reports_to_discord.py" \
  --sessions-dir "${OPENCLAW_HOME}/agents/polymarket/sessions" \
  --codex-index "${CODEX_HOME}/session_index.jsonl" \
  --codex-sessions-root "${CODEX_HOME}/sessions" \
  --state-file "/tmp/poly_codex_discord_bridge/state.json" \
  --workspace "${POLYMARKET_WORKSPACE}" \
  --channel "discord" \
  --target "channel:1483402853648302081" \
  --account "default" \
  --openclaw-bin "${OPENCLAW_BIN}"
