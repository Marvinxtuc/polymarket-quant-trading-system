#!/usr/bin/env bash
set -euo pipefail

BASE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUNTIME_DIR="/tmp/poly_monitor_reports"
PID_FILE="$RUNTIME_DIR/run_monitor_reports.pid"
METHOD_FILE="$RUNTIME_DIR/method"
LOG_PREFIX="scripts/run_monitor_reports.sh"
SCRIPT_PATH="$BASE/scripts/run_monitor_reports.sh"

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

pkill -f "$LOG_PREFIX" >/dev/null 2>&1 || true
pkill -f "/tmp/poly_monitor_reports" >/dev/null 2>&1 || true
pkill -f "$SCRIPT_PATH" >/dev/null 2>&1 || true

if [[ -f "$METHOD_FILE" ]]; then
  rm -f "$METHOD_FILE"
fi

echo "monitor report scheduler stopped."
