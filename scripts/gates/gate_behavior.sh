#!/usr/bin/env bash
set -euo pipefail

BLOCK_ID="${1:-}"
if [[ -z "${BLOCK_ID}" ]]; then
  echo "[gate_behavior] ERROR: missing BLOCK-ID argument" >&2
  exit 2
fi

echo "[gate_behavior] BLOCK-ID=${BLOCK_ID}"

if [[ -x ".venv/bin/python" ]]; then
  PYTHON_BIN=".venv/bin/python"
elif command -v python3 >/dev/null 2>&1; then
  PYTHON_BIN="python3"
else
  echo "[gate_behavior] ERROR: python runtime not found (.venv/bin/python or python3)" >&2
  exit 4
fi

if [[ "${BLOCK_ID}" == "BLOCK-001" ]]; then
  "${PYTHON_BIN}" scripts/verify_runtime_persistence.py
  "${PYTHON_BIN}" scripts/verify_restart_recovery.py
  echo "[gate_behavior] PASS"
  exit 0
fi

if [[ "${BLOCK_ID}" == "BLOCK-002" ]]; then
  "${PYTHON_BIN}" scripts/verify_idempotent_submission.py
  "${PYTHON_BIN}" scripts/verify_restart_no_duplicate_order.py
  echo "[gate_behavior] PASS"
  exit 0
fi

if [[ "${BLOCK_ID}" == "BLOCK-003" ]]; then
  "${PYTHON_BIN}" scripts/verify_single_writer.py
  "${PYTHON_BIN}" scripts/verify_lock_recovery.py
  echo "[gate_behavior] PASS"
  exit 0
fi

if [[ "${BLOCK_ID}" == "BLOCK-004" ]]; then
  "${PYTHON_BIN}" scripts/verify_fail_closed_startup.py
  "${PYTHON_BIN}" scripts/verify_untrusted_state_blocks_buy.py
  echo "[gate_behavior] PASS"
  exit 0
fi

if [[ "${BLOCK_ID}" == "BLOCK-005" ]]; then
  "${PYTHON_BIN}" scripts/verify_kill_switch_terminal.py
  "${PYTHON_BIN}" scripts/verify_reduce_only_terminal_cleanup.py
  echo "[gate_behavior] PASS"
  exit 0
fi

if [[ "${BLOCK_ID}" == "BLOCK-006" ]]; then
  "${PYTHON_BIN}" scripts/verify_control_auth.py
  "${PYTHON_BIN}" scripts/verify_write_api_local_only.py
  echo "[gate_behavior] PASS"
  exit 0
fi

if [[ "${BLOCK_ID}" == "BLOCK-007" ]]; then
  "${PYTHON_BIN}" scripts/verify_signer_required_live.py
  "${PYTHON_BIN}" scripts/verify_no_raw_key_in_live_mode.py
  echo "[gate_behavior] PASS"
  exit 0
fi

if [[ "${BLOCK_ID}" == "BLOCK-008" ]]; then
  "${PYTHON_BIN}" scripts/verify_metrics_and_alerts.py
  "${PYTHON_BIN}" scripts/verify_heartbeat_staleness_alert.py
  echo "[gate_behavior] PASS"
  exit 0
fi

if [[ "${BLOCK_ID}" == "BLOCK-009" ]]; then
  "${PYTHON_BIN}" scripts/verify_exposure_caps.py
  "${PYTHON_BIN}" scripts/verify_loss_streak_and_drawdown_breakers.py
  echo "[gate_behavior] PASS"
  exit 0
fi

if [[ "${BLOCK_ID}" == "BLOCK-010" ]]; then
  "${PYTHON_BIN}" scripts/verify_release_readiness.py --self-test
  bash scripts/gates/gate_release_readiness.sh
  echo "[gate_behavior] PASS"
  exit 0
fi

echo "[gate_behavior] FAIL: unsupported BLOCK-ID for behavior gate: ${BLOCK_ID}" >&2
exit 3
