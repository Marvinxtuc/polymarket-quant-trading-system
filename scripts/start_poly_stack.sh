#!/bin/bash
set -euo pipefail

BASE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUNTIME_DATA="/tmp/poly_runtime_data"
WEB_PID_FILE="$RUNTIME_DATA/poly_web.pid"
BOT_PID_FILE="$RUNTIME_DATA/poly_bot.pid"
WEB_LOG="$RUNTIME_DATA/poly_web.log"
BOT_LOG="$RUNTIME_DATA/poly_bot.log"
STATE_PATH="$RUNTIME_DATA/state.json"
VERIFY_SCRIPT="$BASE/scripts/verify_stack.sh"
PY_BIN="$BASE/.venv/bin/python"
CURL_BIN="/usr/bin/curl"
LAUNCHCTL_BIN="/bin/launchctl"
UID_NUM="$(id -u)"
WEB_LABEL="ai.poly.web"
BOT_LABEL="ai.poly.bot"
AGENT_DIR="$HOME/Library/LaunchAgents"
WEB_PLIST="$AGENT_DIR/$WEB_LABEL.plist"
BOT_PLIST="$AGENT_DIR/$BOT_LABEL.plist"
STACK_WEB_PORT=8787

read_dotenv_var() {
  local key="$1"
  local dotenv="$BASE/.env"
  [[ -f "$dotenv" ]] || return 0
  awk -F= -v key="$key" '
    $0 ~ "^[[:space:]]*" key "=" {
      sub(/^[[:space:]]*[^=]+=/, "", $0)
      gsub(/^[[:space:]]+|[[:space:]]+$/, "", $0)
      gsub(/^"|"$/, "", $0)
      gsub(/^'\''|'\''$/, "", $0)
      print $0
      exit
    }
  ' "$dotenv"
}

CONTROL_TOKEN="${POLY_CONTROL_TOKEN:-$(read_dotenv_var POLY_CONTROL_TOKEN)}"
WEB_URL="http://127.0.0.1:8787/api/state"
if [[ -n "${CONTROL_TOKEN:-}" ]]; then
  WEB_URL="${WEB_URL}?token=${CONTROL_TOKEN}"
fi

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

launchctl_domain_label() {
  local label="$1"
  echo "gui/$UID_NUM/$label"
}

launchctl_has_service() {
  local label="$1"
  "$LAUNCHCTL_BIN" print "$(launchctl_domain_label "$label")" >/dev/null 2>&1
}

launchctl_stop_service() {
  local label="$1"
  "$LAUNCHCTL_BIN" bootout "$(launchctl_domain_label "$label")" >/dev/null 2>&1 || true
}

proc_cmd() {
  local pid="$1"
  ps -p "$pid" -o command= 2>/dev/null || true
}

list_port_listeners() {
  if command -v lsof >/dev/null 2>&1; then
    lsof -nP -iTCP:"$STACK_WEB_PORT" -sTCP:LISTEN -t 2>/dev/null || true
    return 0
  fi

  if command -v fuser >/dev/null 2>&1; then
    fuser -n tcp "$STACK_WEB_PORT" 2>/dev/null || true
    return 0
  fi

  return 0
}

kill_port_listeners() {
  local pid
  local killed=0
  while IFS= read -r pid; do
    [[ -z "${pid:-}" ]] && continue
    kill "$pid" >/dev/null 2>&1 || true
    killed=1
  done < <(list_port_listeners)

  if [[ "$killed" -eq 1 ]]; then
    sleep 1
  fi
}

stop_pid_file_if_stale() {
  local pid_file="$1"
  local expected_sub="$2"
  if is_running "$pid_file"; then
    local pid cmd
    pid="$(cat "$pid_file" 2>/dev/null || true)"
    cmd="$(proc_cmd "$pid")"
    if [[ "$cmd" != *"$expected_sub"* ]]; then
      kill "$pid" >/dev/null 2>&1 || true
      rm -f "$pid_file"
    fi
  fi
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

is_expected_process() {
  local pid_file="$1"
  local expected_sub="$2"

  if ! is_running "$pid_file"; then
    return 1
  fi

  local pid
  local cmd
  pid="$(cat "$pid_file" 2>/dev/null || true)"
  cmd="$(proc_cmd "$pid")"
  [[ "$cmd" == *"$expected_sub"* ]]
}

stop_legacy_runtime() {
  # Clean previous embedded runtime that also used port 8787.
  stop_pid_file_if_stale "$WEB_PID_FILE" "polymarket_bot.web --host 127.0.0.1 --port $STACK_WEB_PORT"
  stop_pid_file_if_stale "$WEB_PID_FILE" "web.py --host 127.0.0.1 --port $STACK_WEB_PORT"
  stop_pid_file_if_stale "$BOT_PID_FILE" "polymarket_bot.daemon"
  stop_pid_file_if_stale "$BOT_PID_FILE" "daemon.py --state-path $STATE_PATH"

  launchctl_stop_service "$WEB_LABEL"
  launchctl_stop_service "$BOT_LABEL"

  if command -v pgrep >/dev/null 2>&1; then
    # Legacy launch patterns from older snapshot/release.
    for pid in $(pgrep -af "backend/web.py --port $STACK_WEB_PORT|backend/main.py --config|polymarket_bot.web|web.py --host 127.0.0.1 --port $STACK_WEB_PORT|daemon.py --state-path $STATE_PATH" 2>/dev/null | awk '{print $1}'); do
      kill "$pid" >/dev/null 2>&1 || true
    done
  fi
  pkill -f "polymarket_bot.web --host 127.0.0.1 --port 8787" >/dev/null 2>&1 || true
  pkill -f "web.py --host 127.0.0.1 --port 8787" >/dev/null 2>&1 || true
  pkill -f "python -m polymarket_bot.web --host 127.0.0.1 --port $STACK_WEB_PORT" >/dev/null 2>&1 || true
  pkill -f "python3 -m polymarket_bot.web --host 127.0.0.1 --port $STACK_WEB_PORT" >/dev/null 2>&1 || true
  pkill -f "polymarket_bot.web --port $STACK_WEB_PORT" >/dev/null 2>&1 || true
  pkill -f "polymarket_bot.daemon --state-path $STATE_PATH" >/dev/null 2>&1 || true
  pkill -f "polymarket_bot.daemon" >/dev/null 2>&1 || true
  pkill -f "python3 -m polymarket_bot.daemon --state-path $STATE_PATH" >/dev/null 2>&1 || true
  pkill -f "daemon.py --state-path $STATE_PATH" >/dev/null 2>&1 || true
  pkill -f "PYTHONPATH=.*polymarket_bot.web" >/dev/null 2>&1 || true
  pkill -f "PYTHONPATH=.*polymarket_bot.daemon" >/dev/null 2>&1 || true
  kill_port_listeners
  sleep 1
}

wait_for_port_free() {
  local max_attempts=15
  local attempt

  for attempt in $(seq 1 "$max_attempts"); do
    if [[ -z "$(list_port_listeners | tr -d '\n')" ]]; then
      return 0
    fi
    sleep 1
  done

  local listeners
  listeners="$(list_port_listeners | tr '\n' ' ')"
  echo "port $STACK_WEB_PORT is still occupied by pids: ${listeners:-<unknown>}" >&2
  return 1
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
    <string>$BASE/src/polymarket_bot/web.py</string>
    <string>--host</string><string>127.0.0.1</string>
    <string>--port</string><string>8787</string>
    <string>--state-path</string><string>$STATE_PATH</string>
    <string>--frontend-dir</string><string>$BASE/frontend</string>
  </array>
  <key>EnvironmentVariables</key>
  <dict>
    <key>PYTHONPATH</key><string>$BASE/src</string>
    <key>POLY_CONTROL_TOKEN</key><string>${CONTROL_TOKEN}</string>
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
    <string>$BASE/src/polymarket_bot/daemon.py</string>
    <string>--state-path</string><string>$STATE_PATH</string>
  </array>
  <key>EnvironmentVariables</key>
  <dict>
    <key>PYTHONPATH</key><string>$BASE/src</string>
    <key>POLY_CONTROL_TOKEN</key><string>${CONTROL_TOKEN}</string>
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
  "$LAUNCHCTL_BIN" bootout "$(launchctl_domain_label "$label")" >/dev/null 2>&1 || true
  "$LAUNCHCTL_BIN" bootstrap "gui/$UID_NUM" "$plist"
  "$LAUNCHCTL_BIN" kickstart -k "$(launchctl_domain_label "$label")"
}

launch_agent_pid() {
  local label="$1"
  "$LAUNCHCTL_BIN" print "$(launchctl_domain_label "$label")" 2>/dev/null | awk '/^[[:space:]]*pid = /{print $3; exit}'
}

start_direct_process() {
  local pid_file="$1"
  local log_file="$2"
  shift 2

  if is_running "$pid_file"; then
    local old_pid=""
    old_pid="$(cat "$pid_file" 2>/dev/null || true)"
    [[ -n "${old_pid:-}" ]] && kill "$old_pid" >/dev/null 2>&1 || true
    rm -f "$pid_file"
  fi

  nohup env "PYTHONPATH=$BASE/src" "POLY_CONTROL_TOKEN=${CONTROL_TOKEN}" "$@" >>"$log_file" 2>&1 < /dev/null &
  local pid=$!
  echo "$pid" >"$pid_file"
}

start_stack() {
  local use_launchctl=1
  if [[ "${START_STACK_DISABLE_LAUNCHCTL:-0}" == "1" ]]; then
    use_launchctl=0
  fi

  if [[ ! -x "$LAUNCHCTL_BIN" ]]; then
    use_launchctl=0
  elif [[ ! -w "$AGENT_DIR" ]] && ! launchctl_has_service "$WEB_LABEL" && ! launchctl_has_service "$BOT_LABEL"; then
    use_launchctl=0
  fi

  if [[ "$use_launchctl" -eq 1 ]]; then
    launchctl_stop_service "$WEB_LABEL"
    launchctl_stop_service "$BOT_LABEL"

    if ! launchctl_has_service "$WEB_LABEL" && ! launchctl_has_service "$BOT_LABEL" && [[ -w "$AGENT_DIR" ]]; then
      if ! (write_web_plist && write_bot_plist); then
        echo "launchctl plist write failed, fallback to direct start"
        use_launchctl=0
      fi
    fi

    if [[ "$use_launchctl" -eq 1 ]]; then
      if ! (launch_agent_restart "$WEB_LABEL" "$WEB_PLIST" && launch_agent_restart "$BOT_LABEL" "$BOT_PLIST"); then
        echo "launchctl restart failed, fallback to direct start"
        use_launchctl=0
      fi
    fi

    if [[ "$use_launchctl" -eq 1 ]] && ! launchctl_has_service "$WEB_LABEL"; then
      echo "launchctl web label not active, fallback to direct start"
      use_launchctl=0
    fi
    if [[ "$use_launchctl" -eq 1 ]] && ! launchctl_has_service "$BOT_LABEL"; then
      echo "launchctl bot label not active, fallback to direct start"
      use_launchctl=0
    fi
  fi

  if [[ "$use_launchctl" -eq 1 ]]; then
    echo "stack_runtime=launchctl"
    local web_pid
    local bot_pid
    web_pid="$(launch_agent_pid "$WEB_LABEL" || true)"
    bot_pid="$(launch_agent_pid "$BOT_LABEL" || true)"
    [[ -n "${web_pid:-}" ]] && echo "$web_pid" >"$WEB_PID_FILE"
    [[ -n "${bot_pid:-}" ]] && echo "$bot_pid" >"$BOT_PID_FILE"

    if [[ -z "${web_pid:-}" ]] || [[ -z "${bot_pid:-}" ]]; then
      echo "launchctl path failed, fallback to direct start"
      use_launchctl=0
    fi
  fi

  if [[ "$use_launchctl" -eq 0 ]]; then
    echo "stack_runtime=direct"
    start_direct_process \
      "$WEB_PID_FILE" \
      "$WEB_LOG" \
      "$PY_BIN" "$BASE/src/polymarket_bot/web.py" \
      --host 127.0.0.1 \
      --port 8787 \
      --state-path "$STATE_PATH" \
      --frontend-dir "$BASE/frontend"

    start_direct_process \
      "$BOT_PID_FILE" \
      "$BOT_LOG" \
      "$PY_BIN" "$BASE/src/polymarket_bot/daemon.py" \
      --state-path "$STATE_PATH"
  fi
}

cd "$BASE"
if [[ ! -x "$PY_BIN" ]]; then
  echo ".venv/bin/python not found, please create venv first" >&2
  exit 1
fi

RUN_START_TS="$(date +%s)"

if ! "$PY_BIN" "$BASE/scripts/check_env.py" --warn-only; then
  echo "env check failed (continuing)" >&2
fi

kill_port_listeners

ensure_owned_or_stop "$WEB_PID_FILE" "polymarket_bot.web --host 127.0.0.1 --port $STACK_WEB_PORT"
ensure_owned_or_stop "$WEB_PID_FILE" "web.py --host 127.0.0.1 --port $STACK_WEB_PORT"
ensure_owned_or_stop "$BOT_PID_FILE" "polymarket_bot.daemon --state-path $STATE_PATH"
ensure_owned_or_stop "$BOT_PID_FILE" "daemon.py --state-path $STATE_PATH"
stop_legacy_runtime
if ! wait_for_port_free; then
  exit 1
fi
if [[ -f "$STATE_PATH" ]]; then
  cp "$STATE_PATH" "$STATE_PATH.before-start.$RUN_START_TS" 2>/dev/null || true
fi
rm -f "$STATE_PATH"

start_stack

# Verify processes are still alive after spawn.
if ! is_expected_process "$WEB_PID_FILE" "polymarket_bot.web" && ! is_expected_process "$WEB_PID_FILE" "web.py"; then
  echo "web process exited immediately"
  [[ -f "$WEB_LOG" ]] && tail -n 60 "$WEB_LOG"
  exit 1
fi
if ! is_expected_process "$BOT_PID_FILE" "polymarket_bot.daemon" && ! is_expected_process "$BOT_PID_FILE" "daemon.py"; then
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

# Make sure the listener belongs to our running web process, not an old runtime.
if ! is_expected_process "$WEB_PID_FILE" "--host 127.0.0.1 --port 8787"; then
  echo "health check passed but polymarket_bot.web process not found"
  exit 1
fi

if [[ ! -x "$VERIFY_SCRIPT" ]]; then
  echo "verify script missing or not executable: $VERIFY_SCRIPT"
  exit 1
fi

echo "--- runtime verify ---"
VERIFY_STARTED_AT="$RUN_START_TS" \
VERIFY_RETRIES="${START_STACK_VERIFY_RETRIES:-60}" \
VERIFY_RETRY_INTERVAL_SECONDS="${START_STACK_VERIFY_INTERVAL_SECONDS:-3}" \
"$VERIFY_SCRIPT" "$WEB_URL"

echo "web_pid=$(cat "$WEB_PID_FILE" 2>/dev/null || true)"
echo "bot_pid=$(cat "$BOT_PID_FILE" 2>/dev/null || true)"
echo "url=http://127.0.0.1:8787"
