#!/usr/bin/env bash
set -euo pipefail

BASE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LAUNCHCTL_BIN="/bin/launchctl"
LAUNCHD_DIR="$HOME/Library/LaunchAgents"
LABEL="com.poly.market.monitor-reports"
PLIST="$LAUNCHD_DIR/$LABEL.plist"
UID_NUM="$(id -u)"
RUNTIME_DIR="/tmp/poly_monitor_reports"
MODE="${MONITOR_MODE:-both}"
ROTATE_KEEP="${ROTATE_KEEP:-24}"
METHOD_FILE="$RUNTIME_DIR/method"
START_TS="$(date '+%Y-%m-%d %H:%M:%S %Z')"

if [[ ! -x "$LAUNCHCTL_BIN" ]]; then
  echo "launchctl not available" >&2
  exit 1
fi

mkdir -p "$RUNTIME_DIR"
if [[ ! -d "$LAUNCHD_DIR" ]]; then
  mkdir -p "$LAUNCHD_DIR"
fi
if [[ ! -w "$LAUNCHD_DIR" ]]; then
  echo "LaunchAgents directory not writable: $LAUNCHD_DIR" >&2
  echo "Fallback to nohup background mode: run monitor reports directly via launch script." >&2

  # ensure old launcher is stopped before fallback start
  "$BASE/scripts/stop_monitor_reports.sh" >/dev/null 2>&1 || true

  nohup /bin/bash "$BASE/scripts/run_monitor_reports.sh" "$MODE" \
    > "$RUNTIME_DIR/monitor-reports-nohup.log" \
    2>&1 < /dev/null &
  nohup_pid=$!
  cat > "$METHOD_FILE" <<EOF_METHOD
mode=nohup
started=$START_TS
pid=$nohup_pid
EOF_METHOD
  echo "monitor reports started via nohup: mode=$MODE pid=$nohup_pid"
  echo "nohup_stdout=$RUNTIME_DIR/monitor-reports-nohup.log"
  exit 0
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
    <string>/usr/bin:/bin:/usr/sbin:/sbin</string>
    <key>ROTATE_KEEP</key>
    <string>$ROTATE_KEEP</string>
  </dict>
  <key>StandardOutPath</key><string>$RUNTIME_DIR/monitor-reports-stdout.log</string>
  <key>StandardErrorPath</key><string>$RUNTIME_DIR/monitor-reports-stderr.log</string>
</dict>
</plist>
EOF_INNER

"$LAUNCHCTL_BIN" bootout "gui/$UID_NUM/$LABEL" >/dev/null 2>&1 || true
if "$LAUNCHCTL_BIN" bootstrap "gui/$UID_NUM" "$PLIST" >/dev/null && "$LAUNCHCTL_BIN" kickstart -k "gui/$UID_NUM/$LABEL" >/dev/null; then
  sleep 0.5
  monitor_pid="$(cat "$RUNTIME_DIR/run_monitor_reports.pid" 2>/dev/null || true)"
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

echo "failed to install launchd monitor scheduler for com.poly.market.monitor-reports" >&2
exit 1
