#!/usr/bin/env bash
set -euo pipefail

# Tests gate: run block-specific unit/integration regression in fail-closed mode.

BLOCK_ID="${1:-}"
if [[ -z "${BLOCK_ID}" ]]; then
  echo "[gate_tests] ERROR: missing BLOCK-ID argument" >&2
  exit 2
fi

echo "[gate_tests] BLOCK-ID=${BLOCK_ID}"

export PYTHONPATH="src"
if [[ -x ".venv/bin/python" ]]; then
  PYTHON_BIN=".venv/bin/python"
elif command -v python3 >/dev/null 2>&1; then
  PYTHON_BIN="python3"
else
  echo "[gate_tests] ERROR: python runtime not found (.venv/bin/python or python3)" >&2
  exit 3
fi

if [[ "${BLOCK_ID}" == "CTRL-000" ]]; then
  "${PYTHON_BIN}" -m unittest discover -s tests -p "test_gate_smoke_ctrl000.py"
  echo "[gate_tests] PASS"
  exit 0
fi

if [[ "${BLOCK_ID}" == "BLOCK-001" ]]; then
  "${PYTHON_BIN}" -m unittest discover -s tests -p "test_runtime_state_persistence.py"
  "${PYTHON_BIN}" -m unittest discover -s tests -p "test_restart_recovery.py"
  "${PYTHON_BIN}" -m unittest discover -s tests -p "test_tmp_deletion_recovery.py"
  "${PYTHON_BIN}" -m unittest discover -s tests -p "test_broker_db_conflict_blocks_buy.py"
  "${PYTHON_BIN}" -m unittest discover -s tests -p "test_runner_control.py"
  "${PYTHON_BIN}" -m unittest discover -s tests -p "test_web_api.py"
  "${PYTHON_BIN}" -m unittest discover -s tests -p "test_paper_runner.py"
  "${PYTHON_BIN}" -m unittest discover -s tests -p "test_paper_broker.py"
  echo "[gate_tests] PASS"
  exit 0
fi

if [[ "${BLOCK_ID}" == "BLOCK-002" ]]; then
  "${PYTHON_BIN}" -m unittest discover -s tests -p "test_idempotent_order_submission.py"
  "${PYTHON_BIN}" -m unittest discover -s tests -p "test_duplicate_executor_same_signal.py"
  "${PYTHON_BIN}" -m unittest discover -s tests -p "test_timeout_retry_reuses_same_intent.py"
  "${PYTHON_BIN}" -m unittest discover -s tests -p "test_restart_does_not_duplicate_orders.py"
  "${PYTHON_BIN}" -m unittest discover -s tests -p "test_runner_control.py"
  "${PYTHON_BIN}" -m unittest discover -s tests -p "test_paper_runner.py"
  "${PYTHON_BIN}" -m unittest discover -s tests -p "test_paper_broker.py"
  echo "[gate_tests] PASS"
  exit 0
fi

if [[ "${BLOCK_ID}" == "BLOCK-003" ]]; then
  "${PYTHON_BIN}" -m unittest discover -s tests -p "test_single_writer_lock.py"
  "${PYTHON_BIN}" -m unittest discover -s tests -p "test_second_executor_is_readonly_or_exit.py"
  "${PYTHON_BIN}" -m unittest discover -s tests -p "test_lock_recovery_after_crash.py"
  "${PYTHON_BIN}" -m unittest discover -s tests -p "test_control_state_cannot_be_overwritten_by_standby.py"
  "${PYTHON_BIN}" -m unittest discover -s tests -p "test_runtime_state_persistence.py"
  "${PYTHON_BIN}" -m unittest discover -s tests -p "test_restart_recovery.py"
  "${PYTHON_BIN}" -m unittest discover -s tests -p "test_tmp_deletion_recovery.py"
  "${PYTHON_BIN}" -m unittest discover -s tests -p "test_runner_control.py"
  "${PYTHON_BIN}" -m unittest discover -s tests -p "test_web_api.py"
  "${PYTHON_BIN}" -m unittest discover -s tests -p "test_daemon_state.py"
  echo "[gate_tests] PASS"
  exit 0
fi

if [[ "${BLOCK_ID}" == "BLOCK-004" ]]; then
  "${PYTHON_BIN}" -m unittest discover -s tests -p "test_fail_closed_admission.py"
  "${PYTHON_BIN}" -m unittest discover -s tests -p "test_runner_control.py"
  "${PYTHON_BIN}" -m unittest discover -s tests -p "test_web_api.py"
  "${PYTHON_BIN}" -m unittest discover -s tests -p "test_daemon_state.py"
  "${PYTHON_BIN}" -m unittest discover -s tests -p "test_paper_runner.py"
  "${PYTHON_BIN}" -m unittest discover -s tests -p "test_paper_broker.py"
  echo "[gate_tests] PASS"
  exit 0
fi

if [[ "${BLOCK_ID}" == "BLOCK-005" ]]; then
  "${PYTHON_BIN}" -m unittest discover -s tests -p "test_kill_switch_requires_broker_terminal.py"
  "${PYTHON_BIN}" -m unittest discover -s tests -p "test_reduce_only_cancels_pending_buy_and_waits_terminal.py"
  "${PYTHON_BIN}" -m unittest discover -s tests -p "test_cancel_requested_is_not_safe.py"
  "${PYTHON_BIN}" -m unittest discover -s tests -p "test_emergency_stop_latched_until_broker_safe.py"
  "${PYTHON_BIN}" -m unittest discover -s tests -p "test_restart_preserves_kill_switch_inflight_state.py"
  "${PYTHON_BIN}" -m unittest discover -s tests -p "test_runner_control.py"
  "${PYTHON_BIN}" -m unittest discover -s tests -p "test_web_api.py"
  "${PYTHON_BIN}" -m unittest discover -s tests -p "test_paper_runner.py"
  "${PYTHON_BIN}" -m unittest discover -s tests -p "test_paper_broker.py"
  echo "[gate_tests] PASS"
  exit 0
fi

if [[ "${BLOCK_ID}" == "BLOCK-006" ]]; then
  "${PYTHON_BIN}" -m unittest discover -s tests -p "test_control_api_requires_token.py"
  "${PYTHON_BIN}" -m unittest discover -s tests -p "test_empty_token_rejected_in_live_mode.py"
  "${PYTHON_BIN}" -m unittest discover -s tests -p "test_write_api_disabled_when_not_local_or_not_authorized.py"
  "${PYTHON_BIN}" -m unittest discover -s tests -p "test_readonly_api_still_available.py"
  "${PYTHON_BIN}" -m unittest discover -s tests -p "test_control_audit_log_written.py"
  "${PYTHON_BIN}" -m unittest discover -s tests -p "test_web_api.py"
  "${PYTHON_BIN}" -m unittest discover -s tests -p "test_control_state_cannot_be_overwritten_by_standby.py"
  "${PYTHON_BIN}" -m unittest discover -s tests -p "test_runner_control.py"
  echo "[gate_tests] PASS"
  exit 0
fi

if [[ "${BLOCK_ID}" == "BLOCK-007" ]]; then
  "${PYTHON_BIN}" -m unittest discover -s tests -p "test_live_mode_rejects_raw_private_key.py"
  "${PYTHON_BIN}" -m unittest discover -s tests -p "test_signer_required_in_live_mode.py"
  "${PYTHON_BIN}" -m unittest discover -s tests -p "test_signer_failure_blocks_startup.py"
  "${PYTHON_BIN}" -m unittest discover -s tests -p "test_signer_signs_without_exposing_key.py"
  "${PYTHON_BIN}" -m unittest discover -s tests -p "test_hot_wallet_balance_cap_enforced.py"
  "${PYTHON_BIN}" -m unittest discover -s tests -p "test_live_clob.py"
  "${PYTHON_BIN}" -m unittest discover -s tests -p "test_check_env.py"
  "${PYTHON_BIN}" -m unittest discover -s tests -p "test_live_smoke_preflight.py"
  "${PYTHON_BIN}" -m unittest discover -s tests -p "test_runner_control.py"
  "${PYTHON_BIN}" -m unittest discover -s tests -p "test_web_api.py"
  "${PYTHON_BIN}" -m unittest discover -s tests -p "test_paper_runner.py"
  "${PYTHON_BIN}" -m unittest discover -s tests -p "test_paper_broker.py"
  echo "[gate_tests] PASS"
  exit 0
fi

if [[ "${BLOCK_ID}" == "BLOCK-008" ]]; then
  "${PYTHON_BIN}" -m unittest discover -s tests -p "test_metrics_exposed.py"
  "${PYTHON_BIN}" -m unittest discover -s tests -p "test_alert_conditions_derived_from_runtime_state.py"
  "${PYTHON_BIN}" -m unittest discover -s tests -p "test_heartbeat_updates_and_stale_detection.py"
  "${PYTHON_BIN}" -m unittest discover -s tests -p "test_api_state_and_metrics_consistent.py"
  "${PYTHON_BIN}" -m unittest discover -s tests -p "test_web_api.py"
  "${PYTHON_BIN}" -m unittest discover -s tests -p "test_daemon_state.py"
  "${PYTHON_BIN}" -m unittest discover -s tests -p "test_runner_control.py"
  "${PYTHON_BIN}" -m unittest discover -s tests -p "test_signer_required_in_live_mode.py"
  echo "[gate_tests] PASS"
  exit 0
fi

if [[ "${BLOCK_ID}" == "BLOCK-009" ]]; then
  "${PYTHON_BIN}" -m unittest discover -s tests -p "test_exposure_ledger_limits.py"
  "${PYTHON_BIN}" -m unittest discover -s tests -p "test_wallet_portfolio_condition_caps.py"
  "${PYTHON_BIN}" -m unittest discover -s tests -p "test_loss_streak_breaker_blocks_buy.py"
  "${PYTHON_BIN}" -m unittest discover -s tests -p "test_intraday_drawdown_breaker_blocks_buy.py"
  "${PYTHON_BIN}" -m unittest discover -s tests -p "test_risk_breaker_persists_across_restart.py"
  "${PYTHON_BIN}" -m unittest discover -s tests -p "test_api_state_exposes_risk_breakers.py"
  "${PYTHON_BIN}" -m unittest discover -s tests -p "test_runner_control.py"
  "${PYTHON_BIN}" -m unittest discover -s tests -p "test_web_api.py"
  echo "[gate_tests] PASS"
  exit 0
fi

if [[ "${BLOCK_ID}" == "BLOCK-010" ]]; then
  "${PYTHON_BIN}" -m unittest discover -s tests -p "test_release_gate_aggregates_block_results.py"
  "${PYTHON_BIN}" -m unittest discover -s tests -p "test_release_gate_fails_on_any_required_block.py"
  "${PYTHON_BIN}" -m unittest discover -s tests -p "test_release_gate_writes_artifacts.py"
  "${PYTHON_BIN}" -m unittest discover -s tests -p "test_release_gate_machine_and_failclose.py"
  echo "[gate_tests] PASS"
  exit 0
fi

echo "[gate_tests] FAIL: unsupported BLOCK-ID for tests gate: ${BLOCK_ID}" >&2
exit 4
