#!/usr/bin/env bash
set -euo pipefail

RUNTIME_DIR="/tmp/poly_git_autosync"
PID_FILE="$RUNTIME_DIR/git-autosync.pid"
METHOD_FILE="$RUNTIME_DIR/method"
STATUS_FILE="$RUNTIME_DIR/status.json"
UID_NUM="$(id -u)"
LABEL="com.poly.market.git-autosync"

if [[ -f "$METHOD_FILE" ]]; then
  method="$(awk -F= '$1=="mode"{print $2}' "$METHOD_FILE" 2>/dev/null | head -n 1 || true)"
  started="$(awk -F= '$1=="started"{print $2}' "$METHOD_FILE" 2>/dev/null | head -n 1 || true)"
  method_pid="$(awk -F= '$1=="pid"{print $2}' "$METHOD_FILE" 2>/dev/null | head -n 1 || true)"
  echo "git-autosync: mode=${method:-unknown}"
  [[ -n "${started:-}" ]] && echo "started=$started"
  if [[ -n "${method_pid:-}" ]] && kill -0 "$method_pid" >/dev/null 2>&1; then
    echo "pid=$method_pid"
  else
    echo "pid=not-running"
  fi
  if [[ -f "$STATUS_FILE" ]]; then
    echo "status_file=$STATUS_FILE"
    cat "$STATUS_FILE"
  fi
  exit 0
fi

if /bin/launchctl print "gui/$UID_NUM/$LABEL" >/tmp/git-autosync-status.launchctl.out 2>&1; then
  cat /tmp/git-autosync-status.launchctl.out
  rm -f /tmp/git-autosync-status.launchctl.out
  exit 0
fi
rm -f /tmp/git-autosync-status.launchctl.out

if [[ -f "$PID_FILE" ]]; then
  echo "git-autosync: pid file present but method file missing"
else
  echo "git-autosync: not configured"
fi
