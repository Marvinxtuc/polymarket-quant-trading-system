#!/usr/bin/env bash
set -euo pipefail

RUNTIME_DIR="/tmp/poly_monitor_reports"
METHOD_FILE="$RUNTIME_DIR/method"
BASE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PY_BIN="$BASE/.venv/bin/python"
RUNTIME_DIR=""
METHOD_FILE=""
UID_NUM="$(id -u)"
LABEL=""

resolve_runtime_paths() {
  local runtime_py="$PY_BIN"
  if [[ ! -x "$runtime_py" ]]; then
    runtime_py="$(command -v python3)"
  fi
  eval "$("$runtime_py" "$BASE/scripts/runtime_paths.py" --format shell runtime_dir monitor_reports_dir)"
  local mode identity
  mode="$(basename "$(dirname "$RUNTIME_DIR")")"
  identity="$(basename "$RUNTIME_DIR")"
  RUNTIME_DIR="$MONITOR_REPORTS_DIR"
  METHOD_FILE="$RUNTIME_DIR/method"
  LABEL="com.poly.market.monitor-reports.${mode}.${identity}"
}

resolve_runtime_paths

if [[ ! -f "$METHOD_FILE" ]]; then
  if /bin/launchctl print "gui/$UID_NUM/$LABEL" >/tmp/monitor-scheduler-status.launchctl.out 2>&1; then
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
method_log="$(awk -F= '$1=="log"{print $2}' "$METHOD_FILE" 2>/dev/null | head -n 1 || true)"

if [[ "${method:-}" == "nohup" ]]; then
  echo "monitor-scheduler: mode=nohup"
  if [[ -n "${started:-}" ]]; then
    echo "started=$started"
  fi
  if [[ -n "${method_pid:-}" ]] && kill -0 "$method_pid" >/dev/null 2>&1; then
    echo "pid=$method_pid"
    if [[ -n "${method_log:-}" ]]; then
      echo "log=$method_log"
    fi
  else
    echo "status=stale"
    if [[ -n "${method_pid:-}" ]]; then
      echo "stale_pid=$method_pid"
    fi
    if [[ -n "${method_log:-}" ]]; then
      echo "log=$method_log"
    fi
  fi
  exit 0
fi

if [[ "${method:-}" == "launchd" ]]; then
  echo "monitor-scheduler: mode=launchd"
  if [[ -n "${started:-}" ]]; then
    echo "started=$started"
  fi
  /bin/launchctl print "gui/$UID_NUM/$LABEL" || true
  exit 0
fi

echo "monitor-scheduler: mode=$method"
