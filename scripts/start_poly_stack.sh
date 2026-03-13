#!/bin/bash
set -euo pipefail

BASE="/Users/marvin.xa/Desktop/Polymarket"
RUNTIME_DATA="/tmp/poly_runtime_data"
WEB_PID_FILE="$RUNTIME_DATA/poly_web.pid"
BOT_PID_FILE="$RUNTIME_DATA/poly_bot.pid"
WEB_LOG="$RUNTIME_DATA/poly_web.log"
BOT_LOG="$RUNTIME_DATA/poly_bot.log"
STATE_PATH="$RUNTIME_DATA/state.json"
WEB_URL="http://127.0.0.1:8787/api/state"
PY_BIN="$BASE/.venv/bin/python"
CURL_BIN="/usr/bin/curl"
LAUNCHCTL_BIN="/bin/launchctl"
UID_NUM="$(id -u)"
WEB_LABEL="ai.poly.web"
BOT_LABEL="ai.poly.bot"
AGENT_DIR="$HOME/Library/LaunchAgents"
WEB_PLIST="$AGENT_DIR/$WEB_LABEL.plist"
BOT_PLIST="$AGENT_DIR/$BOT_LABEL.plist"

mkdir -p "$RUNTIME_DATA"

is_running() {
  local pid_file="$1"
  if [[ -f "$pid_file" ]]; then
    local pid
    pid="$(cat "$pid_file" 2>/dev/null || true)"
    if [[ -n "${pid:-}" ]] && ps -p "$pid" >/dev/null 2>&1; then
      return 0
    fi
  fi
  return 1
}

proc_cmd() {
  local pid="$1"
  ps -p "$pid" -o command= 2>/dev/null || true
}

ensure_owned_or_stop() {
  local pid_file="$1"
  local expected="$2"
  if is_running "$pid_file"; then
    local pid cmd
    pid="$(cat "$pid_file" 2>/dev/null || true)"
    cmd="$(proc_cmd "$pid")"
    if [[ "$cmd" != *"$expected"* ]]; then
      kill "$pid" >/dev/null 2>&1 || true
      rm -f "$pid_file"
    fi
  fi
}

stop_legacy_runtime() {
  # Clean previous embedded runtime that also used port 8787.
  local pids
  pids="$(pgrep -f "backend/web.py --port 8787|backend/main.py --config" || true)"
  if [[ -n "${pids:-}" ]]; then
    kill $pids >/dev/null 2>&1 || true
    sleep 1
  fi
}

write_web_plist() {
  mkdir -p "$AGENT_DIR"
  cat >"$WEB_PLIST" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>$WEB_LABEL</string>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><true/>
  <key>WorkingDirectory</key><string>$BASE</string>
  <key>ProgramArguments</key>
  <array>
    <string>$PY_BIN</string>
    <string>-m</string>
    <string>polymarket_bot.web</string>
    <string>--host</string><string>127.0.0.1</string>
    <string>--port</string><string>8787</string>
    <string>--state-path</string><string>$STATE_PATH</string>
    <string>--frontend-dir</string><string>$BASE/frontend</string>
  </array>
  <key>EnvironmentVariables</key>
  <dict>
    <key>PYTHONPATH</key><string>$BASE/src</string>
  </dict>
  <key>StandardOutPath</key><string>$WEB_LOG</string>
  <key>StandardErrorPath</key><string>$WEB_LOG</string>
</dict>
</plist>
EOF
}

write_bot_plist() {
  mkdir -p "$AGENT_DIR"
  cat >"$BOT_PLIST" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>$BOT_LABEL</string>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><true/>
  <key>WorkingDirectory</key><string>$BASE</string>
  <key>ProgramArguments</key>
  <array>
    <string>$PY_BIN</string>
    <string>-m</string>
    <string>polymarket_bot.daemon</string>
    <string>--state-path</string><string>$STATE_PATH</string>
  </array>
  <key>EnvironmentVariables</key>
  <dict>
    <key>PYTHONPATH</key><string>$BASE/src</string>
  </dict>
  <key>StandardOutPath</key><string>$BOT_LOG</string>
  <key>StandardErrorPath</key><string>$BOT_LOG</string>
</dict>
</plist>
EOF
}

launch_agent_restart() {
  local label="$1"
  local plist="$2"
  "$LAUNCHCTL_BIN" bootout "gui/$UID_NUM/$label" >/dev/null 2>&1 || true
  "$LAUNCHCTL_BIN" bootstrap "gui/$UID_NUM" "$plist"
  "$LAUNCHCTL_BIN" kickstart -k "gui/$UID_NUM/$label"
}

cd "$BASE"
if [[ ! -x "$PY_BIN" ]]; then
  echo ".venv/bin/python not found, please create venv first" >&2
  exit 1
fi

ensure_owned_or_stop "$WEB_PID_FILE" "polymarket_bot.web"
ensure_owned_or_stop "$BOT_PID_FILE" "polymarket_bot.daemon"
stop_legacy_runtime

write_web_plist
write_bot_plist
launch_agent_restart "$WEB_LABEL" "$WEB_PLIST"
launch_agent_restart "$BOT_LABEL" "$BOT_PLIST"
pgrep -f "polymarket_bot.web --host 127.0.0.1 --port 8787" | head -n 1 >"$WEB_PID_FILE" || true
pgrep -f "polymarket_bot.daemon --state-path $STATE_PATH" | head -n 1 >"$BOT_PID_FILE" || true

# Verify processes are still alive after spawn.
if ! is_running "$WEB_PID_FILE"; then
  echo "web process exited immediately"
  [[ -f "$WEB_LOG" ]] && tail -n 60 "$WEB_LOG"
  exit 1
fi
if ! is_running "$BOT_PID_FILE"; then
  echo "bot process exited immediately"
  [[ -f "$BOT_LOG" ]] && tail -n 60 "$BOT_LOG"
  exit 1
fi

# Health-check local API before returning success.
ok=0
for _ in {1..15}; do
  if "$CURL_BIN" -fsS --max-time 2 "$WEB_URL" >/dev/null 2>&1; then
    ok=1
    break
  fi
  sleep 1
done
if [[ "$ok" -ne 1 ]]; then
  echo "health check failed: $WEB_URL"
  echo "--- web log ---"
  [[ -f "$WEB_LOG" ]] && tail -n 80 "$WEB_LOG"
  echo "--- bot log ---"
  [[ -f "$BOT_LOG" ]] && tail -n 80 "$BOT_LOG"
  exit 1
fi

# Make sure the listener is our web module, not an old runtime.
if ! pgrep -f "polymarket_bot.web --host 127.0.0.1 --port 8787" >/dev/null 2>&1; then
  echo "health check passed but polymarket_bot.web process not found"
  exit 1
fi

echo "web_pid=$(cat "$WEB_PID_FILE" 2>/dev/null || true)"
echo "bot_pid=$(cat "$BOT_PID_FILE" 2>/dev/null || true)"
echo "url=http://127.0.0.1:8787"
