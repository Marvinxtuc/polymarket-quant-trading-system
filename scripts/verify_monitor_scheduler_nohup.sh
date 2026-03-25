#!/usr/bin/env bash
set -euo pipefail

BASE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PY_BIN="$BASE/.venv/bin/python"
if [[ ! -x "$PY_BIN" ]]; then
  PY_BIN="$(command -v python3)"
fi

eval "$("$PY_BIN" "$BASE/scripts/runtime_paths.py" --format shell monitor_reports_dir)"
METHOD_FILE="$MONITOR_REPORTS_DIR/method"

echo "==> install forced-nohup monitor scheduler"
MONITOR_FORCE_NOHUP=1 "$BASE/scripts/install_monitor_scheduler.sh" >/tmp/monitor-scheduler-smoke.install.out
cat /tmp/monitor-scheduler-smoke.install.out

echo "==> verify live nohup status"
status_one="$("$BASE/scripts/monitor_scheduler_status.sh")"
echo "$status_one"
if [[ "$status_one" != *"mode=nohup"* ]] || [[ "$status_one" != *"pid="* ]]; then
  echo "expected monitor scheduler to report active nohup pid" >&2
  exit 1
fi

pid="$(awk -F= '$1=="pid"{print $2}' "$METHOD_FILE" 2>/dev/null | head -n 1 || true)"
if [[ -z "${pid:-}" ]]; then
  echo "failed to read nohup pid from $METHOD_FILE" >&2
  exit 1
fi

echo "==> kill current nohup pid to simulate stale method"
kill "$pid" >/dev/null 2>&1 || true
sleep 1

echo "==> verify stale nohup status"
status_two="$("$BASE/scripts/monitor_scheduler_status.sh")"
echo "$status_two"
if [[ "$status_two" != *"status=stale"* ]]; then
  echo "expected monitor scheduler to report stale status after pid exit" >&2
  exit 1
fi

echo "==> reinstall forced-nohup monitor scheduler for healthy final state"
MONITOR_FORCE_NOHUP=1 "$BASE/scripts/install_monitor_scheduler.sh" >/tmp/monitor-scheduler-smoke.reinstall.out
cat /tmp/monitor-scheduler-smoke.reinstall.out

echo "==> final status"
"$BASE/scripts/monitor_scheduler_status.sh"

echo "PASS: monitor scheduler nohup smoke verified"
