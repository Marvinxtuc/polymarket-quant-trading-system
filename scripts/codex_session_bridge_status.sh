#!/bin/bash
set -euo pipefail

LABEL="ai.codex.poly.session-bridge"
UID_NUM="$(id -u)"
STATE_FILE="/tmp/poly_codex_discord_bridge/state.json"

if /bin/launchctl print "gui/$UID_NUM/$LABEL" >/tmp/poly-codex-session-bridge.launchctl.out 2>&1; then
  echo "codex-session-bridge: active"
  cat /tmp/poly-codex-session-bridge.launchctl.out
else
  echo "codex-session-bridge: inactive"
  cat /tmp/poly-codex-session-bridge.launchctl.out || true
fi
rm -f /tmp/poly-codex-session-bridge.launchctl.out

if [[ -f "$STATE_FILE" ]]; then
  echo
  echo "state:"
  cat "$STATE_FILE"
fi
