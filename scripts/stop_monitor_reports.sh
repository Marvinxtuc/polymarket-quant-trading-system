#!/usr/bin/env bash
set -euo pipefail

BASE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PY_BIN="$BASE/.venv/bin/python"
RUNTIME_DIR=""
PID_FILE=""
METHOD_FILE=""
LOG_PREFIX="scripts/run_monitor_reports.sh"
SCRIPT_PATH="$BASE/scripts/run_monitor_reports.sh"
LAUNCHCTL_BIN="/bin/launchctl"
UID_NUM="$(id -u)"
LABEL=""
LEGACY_LABEL="com.poly.market.monitor-reports"

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
  PID_FILE="$RUNTIME_DIR/run_monitor_reports.pid"
  METHOD_FILE="$RUNTIME_DIR/method"
  LABEL="com.poly.market.monitor-reports.${mode}.${identity}"
}

resolve_runtime_paths

if [[ -f "$PID_FILE" ]]; then
  pid="$(cat "$PID_FILE" 2>/dev/null || true)"
  if [[ -n "${pid:-}" ]] && kill -0 "$pid" >/dev/null 2>&1; then
    kill "$pid" >/dev/null 2>&1 || true
    for _ in {1..20}; do
      if kill -0 "$pid" >/dev/null 2>&1; then
        sleep 0.2
      else
        break
      fi
    done
  fi
  rm -f "$PID_FILE"
fi

if [[ -x "$LAUNCHCTL_BIN" ]]; then
  "$LAUNCHCTL_BIN" bootout "gui/$UID_NUM/$LABEL" >/dev/null 2>&1 || true
  "$LAUNCHCTL_BIN" bootout "gui/$UID_NUM/$LEGACY_LABEL" >/dev/null 2>&1 || true
fi

pkill -f "$LOG_PREFIX" >/dev/null 2>&1 || true
pkill -f "$SCRIPT_PATH" >/dev/null 2>&1 || true
pkill -f "/tmp/poly_monitor_scheduler_bundle/scripts/run_monitor_reports.sh" >/dev/null 2>&1 || true
pkill -f "/tmp/poly_monitor_reports" >/dev/null 2>&1 || true

if [[ -f "$METHOD_FILE" ]]; then
  rm -f "$METHOD_FILE"
fi

echo "monitor report scheduler stopped."
