#!/usr/bin/env bash
set -euo pipefail

BASE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
START_SCRIPT="$BASE/scripts/start_poly_stack.sh"
VERIFY_SCRIPT="$BASE/scripts/verify_stack.sh"
REHEARSAL_SCRIPT="$BASE/scripts/rehearse_12h_paper.sh"
PY_BIN="$BASE/.venv/bin/python"
OUT_FILE="${1:-}"
WINDOWS="${2:-24}"
INTERVAL="${3:-3600}"
LOG_FILE="${REHEARSE_24H_LOG:-}"

if [[ ! -x "$START_SCRIPT" ]]; then
  echo "missing start script: $START_SCRIPT" >&2
  exit 1
fi

if [[ ! -x "$VERIFY_SCRIPT" ]]; then
  echo "missing verify script: $VERIFY_SCRIPT" >&2
  exit 1
fi

if [[ ! -x "$REHEARSAL_SCRIPT" ]]; then
  echo "missing rehearsal script: $REHEARSAL_SCRIPT" >&2
  exit 1
fi

export START_STACK_DISABLE_LAUNCHCTL=1
export DRY_RUN=true

if [[ ! -x "$PY_BIN" ]]; then
  echo ".venv/bin/python not found, please create venv first" >&2
  exit 1
fi

eval "$("$PY_BIN" "$BASE/scripts/runtime_paths.py" --format shell rehearsal_24h_dry_run_out_path rehearsal_24h_dry_run_log_path)"
if [[ -z "${OUT_FILE:-}" ]]; then
  OUT_FILE="$REHEARSAL_24H_DRY_RUN_OUT_PATH"
fi
if [[ -z "${LOG_FILE:-}" ]]; then
  LOG_FILE="$REHEARSAL_24H_DRY_RUN_LOG_PATH"
fi

echo "==> starting dry-run stack for rehearsal"
"$START_SCRIPT"

echo "==> verifying dry-run stack"
verify_output="$("$VERIFY_SCRIPT")"
echo "$verify_output"
if [[ "$verify_output" != *"mode=paper"* ]]; then
  echo "dry-run rehearsal expected paper mode but got: $verify_output" >&2
  exit 1
fi

echo "==> starting 24h rehearsal monitor"
nohup bash "$REHEARSAL_SCRIPT" "$OUT_FILE" "$WINDOWS" "$INTERVAL" > "$LOG_FILE" 2>&1 &

echo "rehearsal_started=1"
echo "result=$OUT_FILE"
echo "log=$LOG_FILE"
echo "mode=paper"
