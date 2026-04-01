#!/usr/bin/env bash
set -euo pipefail

BASE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PY_BIN="$BASE/.venv/bin/python"
RUNTIME_DATA=""
WEB_PID_FILE=""
BOT_PID_FILE=""
PUBLIC_STATE_PATH=""
STACK_WEB_PORT=8787
LAUNCHCTL_BIN="/bin/launchctl"
UID_NUM="$(id -u)"
WEB_LABEL="ai.poly.web"
BOT_LABEL="ai.poly.bot"

resolve_runtime_paths() {
  local runtime_py="$PY_BIN"
  if [[ ! -x "$runtime_py" ]]; then
    runtime_py="$(command -v python3)"
  fi
  eval "$("$runtime_py" "$BASE/scripts/runtime_paths.py" --format shell runtime_dir public_state_path)"
  RUNTIME_DATA="$RUNTIME_DIR"
  PUBLIC_STATE_PATH="$PUBLIC_STATE_PATH"
  WEB_PID_FILE="$RUNTIME_DATA/poly_web.pid"
  BOT_PID_FILE="$RUNTIME_DATA/poly_bot.pid"
}

launchctl_domain_label() {
  local label="$1"
  echo "gui/$UID_NUM/$label"
}

bootout_service() {
  local label="$1"
  if [[ -x "$LAUNCHCTL_BIN" ]]; then
    "$LAUNCHCTL_BIN" bootout "$(launchctl_domain_label "$label")" >/dev/null 2>&1 || true
  fi
}

kill_pid_file() {
  local pid_file="$1"
  if [[ -f "$pid_file" ]]; then
    local pid
    pid="$(cat "$pid_file" 2>/dev/null || true)"
    if [[ -n "${pid:-}" ]]; then
      kill "$pid" >/dev/null 2>&1 || true
    fi
    rm -f "$pid_file"
  fi
}

kill_port() {
  if command -v lsof >/dev/null 2>&1; then
    while IFS= read -r pid; do
      [[ -n "${pid:-}" ]] || continue
      kill "$pid" >/dev/null 2>&1 || true
    done < <(lsof -nP -iTCP:"$STACK_WEB_PORT" -sTCP:LISTEN -t 2>/dev/null || true)
  fi
}

kill_patterns() {
  pkill -f "polymarket_bot.web --host 127.0.0.1 --port 8787" >/dev/null 2>&1 || true
  pkill -f "python -m polymarket_bot.web --host 127.0.0.1 --port 8787" >/dev/null 2>&1 || true
  pkill -f "python3 -m polymarket_bot.web --host 127.0.0.1 --port 8787" >/dev/null 2>&1 || true
  pkill -f "polymarket_bot.daemon --state-path $RUNTIME_DATA/state.json" >/dev/null 2>&1 || true
  pkill -f "polymarket_bot.daemon" >/dev/null 2>&1 || true
}

echo "Stopping polymarket stack..."
resolve_runtime_paths
if [[ -x "$BASE/scripts/stop_monitor_reports.sh" ]]; then
  "$BASE/scripts/stop_monitor_reports.sh" >/dev/null 2>&1 || true
fi
bootout_service "$WEB_LABEL"
bootout_service "$BOT_LABEL"
kill_pid_file "$WEB_PID_FILE"
kill_pid_file "$BOT_PID_FILE"
kill_patterns
kill_port
rm -f "$RUNTIME_DATA/state.json" "$RUNTIME_DATA/state.json.tmp" "$RUNTIME_DATA/poly_web.log" "$RUNTIME_DATA/poly_bot.log"
rm -f "$PUBLIC_STATE_PATH"

echo "Stack stop complete."
