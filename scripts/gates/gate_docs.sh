#!/usr/bin/env bash
set -euo pipefail

# Docs gate: verify required report artifacts and docs exist for BLOCK-ID.

BLOCK_ID="${1:-}"
if [[ -z "${BLOCK_ID}" ]]; then
  echo "[gate_docs] ERROR: missing BLOCK-ID argument" >&2
  exit 2
fi

DOC_ROOT="reports/blocking/${BLOCK_ID}"
REQ_FILES=(
  "${DOC_ROOT}/validation.txt"
  "${DOC_ROOT}/self_check.md"
)

if [[ "${BLOCK_ID}" == "BLOCK-001" ]]; then
  REQ_FILES+=(
    "${DOC_ROOT}/regression.txt"
    "docs/runbook/runtime_state_recovery.md"
    "README.md"
  )
fi

if [[ "${BLOCK_ID}" == "BLOCK-002" ]]; then
  REQ_FILES+=(
    "${DOC_ROOT}/regression.txt"
    "docs/runbook/order_idempotency.md"
    "README.md"
  )
fi

if [[ "${BLOCK_ID}" == "BLOCK-003" ]]; then
  REQ_FILES+=(
    "${DOC_ROOT}/regression.txt"
    "docs/runbook/single_writer_lock.md"
    "README.md"
  )
fi

if [[ "${BLOCK_ID}" == "BLOCK-004" ]]; then
  REQ_FILES+=(
    "${DOC_ROOT}/regression.txt"
    "docs/runbook/fail_closed_admission.md"
    "README.md"
  )
fi

if [[ "${BLOCK_ID}" == "BLOCK-005" ]]; then
  REQ_FILES+=(
    "${DOC_ROOT}/regression.txt"
    "docs/runbook/kill_switch_terminal_confirmation.md"
    "README.md"
  )
fi

if [[ "${BLOCK_ID}" == "BLOCK-006" ]]; then
  REQ_FILES+=(
    "${DOC_ROOT}/regression.txt"
    "docs/runbook/control_plane_security.md"
    "README.md"
  )
fi

if [[ "${BLOCK_ID}" == "BLOCK-007" ]]; then
  REQ_FILES+=(
    "${DOC_ROOT}/regression.txt"
    "docs/runbook/signer_and_secret_boundary.md"
    "README.md"
  )
fi

if [[ "${BLOCK_ID}" == "BLOCK-008" ]]; then
  REQ_FILES+=(
    "${DOC_ROOT}/regression.txt"
    "docs/runbook/observability_and_alerting.md"
    "README.md"
  )
fi

if [[ "${BLOCK_ID}" == "BLOCK-009" ]]; then
  REQ_FILES+=(
    "${DOC_ROOT}/regression.txt"
    "docs/runbook/exposure_and_breakers.md"
    "README.md"
  )
fi

if [[ "${BLOCK_ID}" == "BLOCK-010" ]]; then
  REQ_FILES+=(
    "${DOC_ROOT}/regression.txt"
    "docs/runbook/release_gating_and_go_no_go.md"
    "docs/blocking/final_release_checklist.md"
    "reports/release/go_no_go_summary.json"
    "reports/release/go_no_go_summary.md"
    "README.md"
  )
fi

missing=0
for f in "${REQ_FILES[@]}"; do
  if [[ ! -f "${f}" ]]; then
    echo "[gate_docs] MISSING: ${f}" >&2
    missing=1
  elif [[ ! -s "${f}" ]]; then
    echo "[gate_docs] EMPTY: ${f}" >&2
    missing=1
  fi
done

if [[ "${missing}" -ne 0 ]]; then
  echo "[gate_docs] FAIL: required docs missing/empty" >&2
  exit 3
fi

if [[ "${BLOCK_ID}" == "BLOCK-001" ]]; then
  if ! rg -q "runtime_state_recovery|state_store_path|order_intents" README.md; then
    echo "[gate_docs] FAIL: README.md missing BLOCK-001 runtime truth guidance" >&2
    exit 4
  fi
fi

if [[ "${BLOCK_ID}" == "BLOCK-002" ]]; then
  if ! rg -qi "order idempotency|idempotent order|strategy_order_uuid|manual_required" README.md docs/runbook/order_idempotency.md; then
    echo "[gate_docs] FAIL: missing BLOCK-002 idempotency guidance" >&2
    exit 4
  fi
fi

if [[ "${BLOCK_ID}" == "BLOCK-003" ]]; then
  if ! rg -qi "single-writer|writer scope|WALLET_LOCK_PATH|single_writer_conflict|read-only mode" README.md docs/runbook/single_writer_lock.md; then
    echo "[gate_docs] FAIL: missing BLOCK-003 single-writer guidance" >&2
    exit 4
  fi
fi

if [[ "${BLOCK_ID}" == "BLOCK-004" ]]; then
  if ! rg -qi "admission gate|fail[- ]closed|opening_allowed|reason_codes|evidence_summary|reconciliation_fail|halted|reduce_only" README.md docs/runbook/fail_closed_admission.md; then
    echo "[gate_docs] FAIL: missing BLOCK-004 fail-closed admission guidance" >&2
    exit 4
  fi
fi

if [[ "${BLOCK_ID}" == "BLOCK-005" ]]; then
  if ! rg -qi "kill switch|broker terminal|WAITING_BROKER_TERMINAL|SAFE_CONFIRMED|FAILED_MANUAL_REQUIRED|manual_required|cancel_requested" README.md docs/runbook/kill_switch_terminal_confirmation.md; then
    echo "[gate_docs] FAIL: missing BLOCK-005 kill-switch terminal guidance" >&2
    exit 4
  fi
fi

if [[ "${BLOCK_ID}" == "BLOCK-006" ]]; then
  if ! rg -qi "control plane|write_api_available|token_configured|source_policy|trusted proxy|readonly_mode|POLY_ENABLE_WRITE_API|POLY_CONTROL_TOKEN|control_audit" README.md docs/runbook/control_plane_security.md; then
    echo "[gate_docs] FAIL: missing BLOCK-006 control-plane security guidance" >&2
    exit 4
  fi
fi

if [[ "${BLOCK_ID}" == "BLOCK-007" ]]; then
  if ! rg -qi "signer|external_http|PRIVATE_KEY|raw private key|CLOB_API_KEY|CLOB_API_SECRET|CLOB_API_PASSPHRASE|hot wallet cap|live signer" README.md docs/runbook/signer_and_secret_boundary.md; then
    echo "[gate_docs] FAIL: missing BLOCK-007 signer/secret boundary guidance" >&2
    exit 4
  fi
fi

if [[ "${BLOCK_ID}" == "BLOCK-008" ]]; then
  if ! rg -qi "observability|/metrics|heartbeat|alert_code|page|warning|buy_blocked_duration_seconds|runner_heartbeat_stale|admission_fail_closed|writer_conflict_readonly|hot_wallet_cap_exceeded" README.md docs/runbook/observability_and_alerting.md; then
    echo "[gate_docs] FAIL: missing BLOCK-008 observability guidance" >&2
    exit 4
  fi
fi

if [[ "${BLOCK_ID}" == "BLOCK-009" ]]; then
  if ! rg -qi "usd|three cap|cap.*simultaneously|primary reason|loss streak|drawdown|timezone|day cut|breaker|risk fault|fail-closed" README.md docs/runbook/exposure_and_breakers.md; then
    echo "[gate_docs] FAIL: missing BLOCK-009 exposure/breaker guidance" >&2
    exit 4
  fi
fi

if [[ "${BLOCK_ID}" == "BLOCK-010" ]]; then
  if ! rg -qi "go/no-go|required blocks|release gate|final gate|no-go|block-001|block-009|atomic|json|markdown" README.md docs/runbook/release_gating_and_go_no_go.md docs/blocking/final_release_checklist.md; then
    echo "[gate_docs] FAIL: missing BLOCK-010 release-governance guidance" >&2
    exit 4
  fi
  if ! python3 - <<'PY'
import json
from pathlib import Path

json_path = Path("reports/release/go_no_go_summary.json")
md_path = Path("reports/release/go_no_go_summary.md")
data = json.loads(json_path.read_text(encoding="utf-8"))
required_keys = {
    "go_no_go",
    "execution_timestamp_utc",
    "git_branch",
    "git_commit",
    "release_gate_command",
    "required_blocks",
    "blocks",
}
missing = sorted(required_keys - set(data.keys()))
if missing:
    raise SystemExit(f"missing release summary keys: {missing}")
md = md_path.read_text(encoding="utf-8")
if "Decision:" not in md or "Final Verdict" not in md:
    raise SystemExit("release markdown summary missing required sections")
PY
  then
    echo "[gate_docs] FAIL: BLOCK-010 release report structure invalid" >&2
    exit 4
  fi
fi

echo "[gate_docs] PASS: required docs present and non-empty"
exit 0
