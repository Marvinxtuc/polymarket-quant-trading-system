#!/usr/bin/env bash
set -euo pipefail

BASE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PY_BIN="$BASE/.venv/bin/python"
LAUNCHCTL_BIN="/bin/launchctl"
LAUNCHD_DIR="$HOME/Library/LaunchAgents"
UID_NUM="$(id -u)"
LABEL=""
PLIST=""
RUNTIME_DIR=""
MODE="${MONITOR_MODE:-both}"
ROTATE_KEEP="${ROTATE_KEEP:-24}"
METHOD_FILE=""
START_TS="$(date '+%Y-%m-%d %H:%M:%S %Z')"
FORCE_NOHUP="${MONITOR_FORCE_NOHUP:-0}"

resolve_runtime_paths() {
  local runtime_py="$PY_BIN"
  if [[ ! -x "$runtime_py" ]]; then
    runtime_py="$(command -v python3)"
  fi
  eval "$("$runtime_py" "$BASE/scripts/runtime_paths.py" --format shell runtime_dir monitor_reports_dir)"
  local mode identity
  mode="$(basename "$(dirname "$RUNTIME_DIR")")"
  identity="$(basename "$RUNTIME_DIR")"
  LABEL="com.poly.market.monitor-reports.${mode}.${identity}"
  PLIST="$LAUNCHD_DIR/$LABEL.plist"
  RUNTIME_DIR="$MONITOR_REPORTS_DIR"
  METHOD_FILE="$RUNTIME_DIR/method"
}

resolve_runtime_paths

if [[ ! -x "$LAUNCHCTL_BIN" ]]; then
  echo "launchctl not available" >&2
  exit 1
fi

mkdir -p "$RUNTIME_DIR"
if [[ ! -d "$LAUNCHD_DIR" ]]; then
  mkdir -p "$LAUNCHD_DIR"
fi

"$BASE/scripts/stop_monitor_reports.sh" >/dev/null 2>&1 || true

start_nohup_monitor() {
  if [[ -n "${1:-}" ]]; then
    echo "$1" >&2
  fi

  # ensure old launcher is stopped before fallback start
  "$BASE/scripts/stop_monitor_reports.sh" >/dev/null 2>&1 || true

  local nohup_log="$RUNTIME_DIR/monitor-reports-nohup.log"
  nohup /bin/bash "$BASE/scripts/run_monitor_reports.sh" "$MODE" \
    > "$nohup_log" \
    2>&1 < /dev/null &
  nohup_pid=$!
  sleep 1
  if ! kill -0 "$nohup_pid" >/dev/null 2>&1; then
    rm -f "$METHOD_FILE"
    echo "failed to start monitor reports via nohup" >&2
    echo "nohup_stdout=$nohup_log" >&2
    exit 1
  fi
  cat > "$METHOD_FILE" <<EOF_METHOD
mode=nohup
started=$START_TS
pid=$nohup_pid
log=$nohup_log
EOF_METHOD
  echo "monitor reports started via nohup: mode=$MODE pid=$nohup_pid"
  echo "nohup_stdout=$nohup_log"
  exit 0
}

if [[ "$FORCE_NOHUP" == "1" ]]; then
  start_nohup_monitor "MONITOR_FORCE_NOHUP=1, using nohup background mode."
fi

if [[ ! -w "$LAUNCHD_DIR" ]]; then
  start_nohup_monitor "LaunchAgents directory not writable: $LAUNCHD_DIR"
fi

cat > "$PLIST" <<EOF_INNER
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>$LABEL</string>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><true/>
  <key>WorkingDirectory</key><string>$BASE</string>
  <key>ProgramArguments</key>
  <array>
    <string>/bin/bash</string>
    <string>$BASE/scripts/run_monitor_reports.sh</string>
    <string>$MODE</string>
  </array>
  <key>EnvironmentVariables</key>
  <dict>
    <key>PATH</key>
    <string>/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin</string>
    <key>ROTATE_KEEP</key>
    <string>$ROTATE_KEEP</string>
    <key>PYTHONPATH</key>
    <string>$BASE/src</string>
  </dict>
  <key>StandardOutPath</key><string>$RUNTIME_DIR/monitor-reports-stdout.log</string>
  <key>StandardErrorPath</key><string>$RUNTIME_DIR/monitor-reports-stderr.log</string>
</dict>
</plist>
EOF_INNER

"$LAUNCHCTL_BIN" bootout "gui/$UID_NUM/$LABEL" >/dev/null 2>&1 || true
if "$LAUNCHCTL_BIN" bootstrap "gui/$UID_NUM" "$PLIST" >/dev/null && "$LAUNCHCTL_BIN" kickstart -k "gui/$UID_NUM/$LABEL" >/dev/null; then
  sleep 1
  monitor_pid="$(cat "$RUNTIME_DIR/run_monitor_reports.pid" 2>/dev/null || true)"
  if [[ -n "${monitor_pid:-}" ]] && kill -0 "$monitor_pid" >/dev/null 2>&1; then
    cat > "$METHOD_FILE" <<EOF_METHOD
mode=launchd
started=$START_TS
pid=$monitor_pid
EOF_METHOD
    echo "monitor reports launchd installed and started: $LABEL"
    echo "plist=$PLIST"
    echo "mode=$MODE"
    echo "rotate_keep=$ROTATE_KEEP"
    exit 0
  fi

  "$LAUNCHCTL_BIN" bootout "gui/$UID_NUM/$LABEL" >/dev/null 2>&1 || true
  rm -f "$PLIST"
  start_nohup_monitor "launchd monitor scheduler did not stay up; fallback to nohup."
fi

echo "failed to install launchd monitor scheduler for com.poly.market.monitor-reports" >&2
exit 1
