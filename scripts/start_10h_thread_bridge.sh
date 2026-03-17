#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

exec /usr/bin/python3 "$SCRIPT_DIR/bridge_10h_thread_report.py" \
  --thread-id "019cf8af-55cf-7071-a954-1727836ad6dc" \
  --rollout-path "/Users/marvin.xa/.codex/sessions/2026/03/17/rollout-2026-03-17T06-06-06-019cf8af-55cf-7071-a954-1727836ad6dc.jsonl" \
  --threads-db "/Users/marvin.xa/.codex/state_5.sqlite" \
  --automation-db "/Users/marvin.xa/.codex/sqlite/codex-dev.db" \
  --automation-toml "/Users/marvin.xa/.codex/automations/10h/automation.toml" \
  --automation-id "10h" \
  --automation-thread-id "019cf8b3-fb3c-7b93-8d55-57b6ebdea0f7" \
  --rehearsal-file "/tmp/poly_10h_paper_rehearsal.txt" \
  --state-file "/tmp/poly_10h_thread_bridge_state.json" \
  --interval-seconds 300 \
  --pause-automation \
  --archive-automation-thread
