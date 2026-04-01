#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CODEX_HOME="${CODEX_HOME:-$HOME/.codex}"

THREAD_ID="${POLY_10H_THREAD_ID:-019cf8af-55cf-7071-a954-1727836ad6dc}"
ROLLOUT_PATH="${POLY_10H_ROLLOUT_PATH:-$CODEX_HOME/sessions/2026/03/17/rollout-2026-03-17T06-06-06-019cf8af-55cf-7071-a954-1727836ad6dc.jsonl}"
THREADS_DB="${POLY_10H_THREADS_DB:-$CODEX_HOME/state_5.sqlite}"
AUTOMATION_DB="${POLY_10H_AUTOMATION_DB:-$CODEX_HOME/sqlite/codex-dev.db}"
AUTOMATION_TOML="${POLY_10H_AUTOMATION_TOML:-$CODEX_HOME/automations/10h/automation.toml}"
AUTOMATION_ID="${POLY_10H_AUTOMATION_ID:-10h}"
AUTOMATION_THREAD_ID="${POLY_10H_AUTOMATION_THREAD_ID:-019cf8b3-fb3c-7b93-8d55-57b6ebdea0f7}"
REHEARSAL_FILE="${POLY_10H_REHEARSAL_FILE:-/tmp/poly_10h_paper_rehearsal.txt}"
STATE_FILE="${POLY_10H_STATE_FILE:-/tmp/poly_10h_thread_bridge_state.json}"
INTERVAL_SECONDS="${POLY_10H_INTERVAL_SECONDS:-300}"

exec /usr/bin/python3 "$SCRIPT_DIR/bridge_10h_thread_report.py" \
  --thread-id "${THREAD_ID}" \
  --rollout-path "${ROLLOUT_PATH}" \
  --threads-db "${THREADS_DB}" \
  --automation-db "${AUTOMATION_DB}" \
  --automation-toml "${AUTOMATION_TOML}" \
  --automation-id "${AUTOMATION_ID}" \
  --automation-thread-id "${AUTOMATION_THREAD_ID}" \
  --rehearsal-file "${REHEARSAL_FILE}" \
  --state-file "${STATE_FILE}" \
  --interval-seconds "${INTERVAL_SECONDS}" \
  --pause-automation \
  --archive-automation-thread
