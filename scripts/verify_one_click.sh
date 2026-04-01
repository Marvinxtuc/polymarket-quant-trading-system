#!/bin/bash
set -euo pipefail

BASE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$BASE"

if [[ ! -x ".venv/bin/python" ]]; then
  echo ".venv/bin/python not found, please create the virtualenv first" >&2
  exit 1
fi

echo "==> Run unit tests"
./scripts/run_tests.sh

echo "==> Start/refresh local stack"
START_STACK_DISABLE_LAUNCHCTL="${START_STACK_DISABLE_LAUNCHCTL:-0}" ./scripts/start_poly_stack.sh

echo "==> Final stack verify"
./scripts/verify_stack.sh

echo "verify_one_click OK"
