#!/bin/bash
set -euo pipefail

BASE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PY_BIN="$BASE/.venv/bin/python"

cd "$BASE"

if [[ ! -x "$PY_BIN" ]]; then
  echo ".venv/bin/python not found, please create the virtualenv first" >&2
  exit 1
fi

PYTHONPATH="$BASE/src" "$PY_BIN" -m unittest discover -s "$BASE/tests" -v
