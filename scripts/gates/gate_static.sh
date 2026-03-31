#!/usr/bin/env bash
set -euo pipefail

# Static gate: lightweight but real checks, fail-closed on any issue.
# Checks:
#   - required infra files present
#   - bash syntax check on gate scripts

BLOCK_ID="${1:-}"
if [[ -z "${BLOCK_ID}" ]]; then
  echo "[gate_static] ERROR: missing BLOCK-ID argument" >&2
  exit 2
fi

echo "[gate_static] BLOCK-ID=${BLOCK_ID}"

REQUIRED_FILES=(
  "prompts/codex_master_prompt.md"
  "prompts/task_template.md"
  "prompts/retry_template.md"
  "docs/blocking/tasks/README.md"
  "scripts/gates/gate_static.sh"
  "scripts/gates/gate_tests.sh"
  "scripts/gates/gate_behavior.sh"
  "scripts/gates/gate_docs.sh"
  "scripts/gates/gate_block_item.sh"
)

if [[ "${BLOCK_ID}" == "BLOCK-001" ]]; then
  REQUIRED_FILES+=(
    "migrations/001_init_runtime_tables.sql"
    "src/polymarket_bot/state_store.py"
    "src/polymarket_bot/runner.py"
    "src/polymarket_bot/web.py"
    "src/polymarket_bot/models/runtime_state.py"
    "src/polymarket_bot/models/order_intent.py"
    "src/polymarket_bot/models/control_state.py"
    "tests/test_runtime_state_persistence.py"
    "tests/test_restart_recovery.py"
    "tests/test_tmp_deletion_recovery.py"
    "tests/test_broker_db_conflict_blocks_buy.py"
    "scripts/verify_runtime_persistence.py"
    "scripts/verify_restart_recovery.py"
    "docs/runbook/runtime_state_recovery.md"
  )
fi

if [[ "${BLOCK_ID}" == "BLOCK-002" ]]; then
  REQUIRED_FILES+=(
    "migrations/002_order_intent_idempotency.sql"
    "src/polymarket_bot/idempotency.py"
    "src/polymarket_bot/state_store.py"
    "src/polymarket_bot/runner.py"
    "tests/test_idempotent_order_submission.py"
    "tests/test_duplicate_executor_same_signal.py"
    "tests/test_timeout_retry_reuses_same_intent.py"
    "tests/test_restart_does_not_duplicate_orders.py"
    "scripts/verify_idempotent_submission.py"
    "scripts/verify_restart_no_duplicate_order.py"
    "docs/runbook/order_idempotency.md"
  )
fi

if [[ "${BLOCK_ID}" == "BLOCK-003" ]]; then
  REQUIRED_FILES+=(
    "src/polymarket_bot/locks.py"
    "src/polymarket_bot/state_store.py"
    "src/polymarket_bot/runner.py"
    "src/polymarket_bot/main.py"
    "src/polymarket_bot/daemon.py"
    "src/polymarket_bot/web.py"
    "tests/test_single_writer_lock.py"
    "tests/test_second_executor_is_readonly_or_exit.py"
    "tests/test_lock_recovery_after_crash.py"
    "tests/test_control_state_cannot_be_overwritten_by_standby.py"
    "scripts/verify_single_writer.py"
    "scripts/verify_lock_recovery.py"
    "docs/runbook/single_writer_lock.md"
  )
fi

if [[ "${BLOCK_ID}" == "BLOCK-004" ]]; then
  REQUIRED_FILES+=(
    "src/polymarket_bot/admission_gate.py"
    "src/polymarket_bot/runner.py"
    "src/polymarket_bot/web.py"
    "tests/test_fail_closed_admission.py"
    "tests/test_runner_control.py"
    "tests/test_web_api.py"
    "scripts/verify_fail_closed_startup.py"
    "scripts/verify_untrusted_state_blocks_buy.py"
    "docs/runbook/fail_closed_admission.md"
    "README.md"
    "reports/blocking/BLOCK-004/validation.txt"
    "reports/blocking/BLOCK-004/regression.txt"
    "reports/blocking/BLOCK-004/self_check.md"
  )
fi

if [[ "${BLOCK_ID}" == "BLOCK-005" ]]; then
  REQUIRED_FILES+=(
    "src/polymarket_bot/kill_switch.py"
    "src/polymarket_bot/runner.py"
    "src/polymarket_bot/state_store.py"
    "src/polymarket_bot/web.py"
    "src/polymarket_bot/config.py"
    "tests/test_kill_switch_requires_broker_terminal.py"
    "tests/test_reduce_only_cancels_pending_buy_and_waits_terminal.py"
    "tests/test_cancel_requested_is_not_safe.py"
    "tests/test_emergency_stop_latched_until_broker_safe.py"
    "tests/test_restart_preserves_kill_switch_inflight_state.py"
    "scripts/verify_kill_switch_terminal.py"
    "scripts/verify_reduce_only_terminal_cleanup.py"
    "docs/runbook/kill_switch_terminal_confirmation.md"
    "README.md"
    "reports/blocking/BLOCK-005/validation.txt"
    "reports/blocking/BLOCK-005/regression.txt"
    "reports/blocking/BLOCK-005/self_check.md"
  )
fi

if [[ "${BLOCK_ID}" == "BLOCK-006" ]]; then
  REQUIRED_FILES+=(
    "src/polymarket_bot/control_auth.py"
    "src/polymarket_bot/web.py"
    "src/polymarket_bot/config.py"
    "tests/test_control_api_requires_token.py"
    "tests/test_empty_token_rejected_in_live_mode.py"
    "tests/test_write_api_disabled_when_not_local_or_not_authorized.py"
    "tests/test_readonly_api_still_available.py"
    "tests/test_control_audit_log_written.py"
    "scripts/verify_control_auth.py"
    "scripts/verify_write_api_local_only.py"
    "docs/runbook/control_plane_security.md"
    "README.md"
    "reports/blocking/BLOCK-006/validation.txt"
    "reports/blocking/BLOCK-006/regression.txt"
    "reports/blocking/BLOCK-006/self_check.md"
  )
fi

if [[ "${BLOCK_ID}" == "BLOCK-007" ]]; then
  REQUIRED_FILES+=(
    "src/polymarket_bot/config.py"
    "src/polymarket_bot/main.py"
    "src/polymarket_bot/runner.py"
    "src/polymarket_bot/brokers/base.py"
    "src/polymarket_bot/brokers/live_clob.py"
    "src/polymarket_bot/secrets.py"
    "src/polymarket_bot/signer_client.py"
    "src/polymarket_bot/models/signer_status.py"
    "tests/test_live_mode_rejects_raw_private_key.py"
    "tests/test_signer_required_in_live_mode.py"
    "tests/test_signer_failure_blocks_startup.py"
    "tests/test_signer_signs_without_exposing_key.py"
    "tests/test_hot_wallet_balance_cap_enforced.py"
    "scripts/verify_signer_required_live.py"
    "scripts/verify_no_raw_key_in_live_mode.py"
    "docs/runbook/signer_and_secret_boundary.md"
    "README.md"
    "reports/blocking/BLOCK-007/validation.txt"
    "reports/blocking/BLOCK-007/regression.txt"
    "reports/blocking/BLOCK-007/self_check.md"
  )
fi

if [[ "${BLOCK_ID}" == "BLOCK-008" ]]; then
  REQUIRED_FILES+=(
    "src/polymarket_bot/heartbeat.py"
    "src/polymarket_bot/alerts.py"
    "src/polymarket_bot/metrics.py"
    "src/polymarket_bot/runner.py"
    "src/polymarket_bot/daemon.py"
    "src/polymarket_bot/web.py"
    "src/polymarket_bot/config.py"
    "tests/test_metrics_exposed.py"
    "tests/test_alert_conditions_derived_from_runtime_state.py"
    "tests/test_heartbeat_updates_and_stale_detection.py"
    "tests/test_api_state_and_metrics_consistent.py"
    "scripts/verify_metrics_and_alerts.py"
    "scripts/verify_heartbeat_staleness_alert.py"
    "deploy/prometheus/polymarket_rules.yml"
    "deploy/prometheus/alerts_example.yml"
    "docs/runbook/observability_and_alerting.md"
    "README.md"
    "reports/blocking/BLOCK-008/validation.txt"
    "reports/blocking/BLOCK-008/regression.txt"
    "reports/blocking/BLOCK-008/self_check.md"
  )
fi

if [[ "${BLOCK_ID}" == "BLOCK-009" ]]; then
  REQUIRED_FILES+=(
    "scripts/verify_exposure_caps.py"
    "scripts/verify_loss_streak_and_drawdown_breakers.py"
    "tests/test_exposure_ledger_limits.py"
    "tests/test_wallet_portfolio_condition_caps.py"
    "tests/test_loss_streak_breaker_blocks_buy.py"
    "tests/test_intraday_drawdown_breaker_blocks_buy.py"
    "tests/test_risk_breaker_persists_across_restart.py"
    "tests/test_api_state_exposes_risk_breakers.py"
    "tests/test_runner_control.py"
    "tests/test_web_api.py"
    "docs/runbook/exposure_and_breakers.md"
    "README.md"
    "reports/blocking/BLOCK-009/validation.txt"
    "reports/blocking/BLOCK-009/regression.txt"
    "reports/blocking/BLOCK-009/self_check.md"
  )
fi

if [[ "${BLOCK_ID}" == "BLOCK-010" ]]; then
  REQUIRED_FILES+=(
    "scripts/gates/release_blocks.json"
    "scripts/gates/gate_release_readiness.sh"
    "scripts/verify_release_readiness.py"
    "tests/test_release_gate_aggregates_block_results.py"
    "tests/test_release_gate_fails_on_any_required_block.py"
    "tests/test_release_gate_writes_artifacts.py"
    "tests/test_release_gate_machine_and_failclose.py"
    "docs/runbook/release_gating_and_go_no_go.md"
    "docs/blocking/final_release_checklist.md"
    "README.md"
    "reports/blocking/BLOCK-010/validation.txt"
    "reports/blocking/BLOCK-010/regression.txt"
    "reports/blocking/BLOCK-010/self_check.md"
    "reports/release/go_no_go_summary.json"
    "reports/release/go_no_go_summary.md"
  )
fi

missing=0
for f in "${REQUIRED_FILES[@]}"; do
  if [[ ! -f "${f}" ]]; then
    echo "[gate_static] MISSING ${f}" >&2
    missing=1
  fi
done

if [[ ${missing} -ne 0 ]]; then
  echo "[gate_static] FAIL: required infra files missing" >&2
  exit 3
fi

# Bash syntax check for gate scripts
for f in scripts/gates/gate_static.sh scripts/gates/gate_tests.sh scripts/gates/gate_behavior.sh scripts/gates/gate_docs.sh scripts/gates/gate_block_item.sh scripts/gates/gate_release_readiness.sh; do
  if ! bash -n "${f}"; then
    echo "[gate_static] FAIL: bash -n failed for ${f}" >&2
    exit 4
  fi
done

echo "[gate_static] PASS"
exit 0
