#!/usr/bin/env bash
set -euo pipefail

# Orchestrates blocking-item gates in fail-closed mode.
# Usage: gate_block_item.sh BLOCK-ID

BLOCK_ID="${1:-}"
if [[ -z "${BLOCK_ID}" ]]; then
  echo "[gate_block_item] ERROR: missing BLOCK-ID argument" >&2
  exit 2
fi

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
REPORT_DIR="${ROOT_DIR}/reports/blocking/${BLOCK_ID}"
mkdir -p "${REPORT_DIR}"

echo "[gate_block_item] START BLOCK-ID=${BLOCK_ID}"

static_status=0
tests_status=0
behavior_status=0
docs_status=0

"${ROOT_DIR}/scripts/gates/gate_static.sh" "${BLOCK_ID}" || static_status=$?

"${ROOT_DIR}/scripts/gates/gate_tests.sh" "${BLOCK_ID}" || tests_status=$?

"${ROOT_DIR}/scripts/gates/gate_behavior.sh" "${BLOCK_ID}" || behavior_status=$?

"${ROOT_DIR}/scripts/gates/gate_docs.sh" "${BLOCK_ID}" || docs_status=$?

overall=0
if [[ ${static_status} -ne 0 || ${tests_status} -ne 0 || ${behavior_status} -ne 0 || ${docs_status} -ne 0 ]]; then
  overall=1
fi

echo "[gate_block_item] RESULTS static=${static_status} tests=${tests_status} behavior=${behavior_status} docs=${docs_status} overall=${overall}"
echo "GATE_BLOCK_RESULT block_id=${BLOCK_ID} static=${static_status} tests=${tests_status} behavior=${behavior_status} docs=${docs_status} overall=${overall}"

if [[ ${overall} -ne 0 ]]; then
  exit ${overall}
fi
exit 0
