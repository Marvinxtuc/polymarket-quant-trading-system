#!/usr/bin/env bash
set -euo pipefail

RUNTIME_DIR="/tmp/poly_monitor_reports"
METHOD_FILE="$RUNTIME_DIR/method"
UID_NUM="$(id -u)"

if [[ ! -f "$METHOD_FILE" ]]; then
  if /bin/launchctl print "gui/$UID_NUM/com.poly.market.monitor-reports" >/tmp/monitor-scheduler-status.launchctl.out 2>&1; then
    cat /tmp/monitor-scheduler-status.launchctl.out
    rm -f /tmp/monitor-scheduler-status.launchctl.out
    exit 0
  fi
  rm -f /tmp/monitor-scheduler-status.launchctl.out
  echo "monitor-scheduler: not configured"
  exit 0
fi

method="$(awk -F= '$1=="mode"{print $2}' "$METHOD_FILE" 2>/dev/null | head -n 1 || true)"
started="$(awk -F= '$1=="started"{print $2}' "$METHOD_FILE" 2>/dev/null | head -n 1 || true)"
method_pid="$(awk -F= '$1=="pid"{print $2}' "$METHOD_FILE" 2>/dev/null | head -n 1 || true)"

if [[ "${method:-}" == "nohup" ]]; then
  echo "monitor-scheduler: mode=nohup"
  if [[ -n "${started:-}" ]]; then
    echo "started=$started"
  fi
  if [[ -n "${method_pid:-}" ]] && kill -0 "$method_pid" >/dev/null 2>&1; then
    echo "pid=$method_pid"
  else
    echo "not running"
  fi
  exit 0
fi

if [[ "${method:-}" == "launchd" ]]; then
  echo "monitor-scheduler: mode=launchd"
  if [[ -n "${started:-}" ]]; then
    echo "started=$started"
  fi
  /bin/launchctl print "gui/$UID_NUM/com.poly.market.monitor-reports" || true
  exit 0
fi

echo "monitor-scheduler: mode=$method"
