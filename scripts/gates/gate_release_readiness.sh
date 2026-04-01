#!/usr/bin/env bash
set -euo pipefail

# Final release readiness gate (BLOCK-010)
# Fail-closed: any required block failure or release gate error -> NO-GO.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CONFIG_PATH="${ROOT_DIR}/scripts/gates/release_blocks.json"
JSON_OUT="${ROOT_DIR}/reports/release/go_no_go_summary.json"
MD_OUT="${ROOT_DIR}/reports/release/go_no_go_summary.md"
COMMAND_STR="bash scripts/gates/gate_release_readiness.sh"

if [[ -x "${ROOT_DIR}/.venv/bin/python" ]]; then
  PYTHON_BIN="${ROOT_DIR}/.venv/bin/python"
elif command -v python3 >/dev/null 2>&1; then
  PYTHON_BIN="python3"
else
  echo "[gate_release_readiness] ERROR: python runtime not found" >&2
  exit 3
fi

echo "[gate_release_readiness] START"

"${PYTHON_BIN}" "${ROOT_DIR}/scripts/verify_release_readiness.py" \
  --root "${ROOT_DIR}" \
  --config "${CONFIG_PATH}" \
  --json-out "${JSON_OUT}" \
  --md-out "${MD_OUT}" \
  --release-gate-command "${COMMAND_STR}"
status=$?

if [[ ${status} -eq 0 ]]; then
  echo "[gate_release_readiness] RESULT=GO"
else
  echo "[gate_release_readiness] RESULT=NO-GO"
fi

exit ${status}
