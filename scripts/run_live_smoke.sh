#!/usr/bin/env bash
set -euo pipefail

BASE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PY_BIN="${LIVE_SMOKE_PY_BIN:-$BASE/.venv/bin/python}"

if [[ ! -x "$PY_BIN" ]]; then
  echo ".venv/bin/python not found" >&2
  exit 1
fi

TOKEN_ID="${1:-${LIVE_SMOKE_TOKEN_ID:-}}"
RESTING_USD="${LIVE_SMOKE_RESTING_USD:-1.0}"
AGGRESSIVE_USD="${LIVE_SMOKE_AGGRESSIVE_USD:-1.0}"
SLEEP_SECONDS="${LIVE_SMOKE_SLEEP_SECONDS:-2.0}"
MAX_USD="${LIVE_SMOKE_MAX_USD:-2.0}"
ACK="${LIVE_SMOKE_ACK:-}"
SUMMARY_PATH="${LIVE_SMOKE_SUMMARY_PATH:-}"
LOG_PATH="${LIVE_SMOKE_LOG_PATH:-}"

if [[ -z "${TOKEN_ID:-}" ]]; then
  echo "missing token id: pass as first arg or set LIVE_SMOKE_TOKEN_ID" >&2
  exit 1
fi

if [[ "$ACK" != "YES" ]]; then
  echo "refusing live smoke without LIVE_SMOKE_ACK=YES" >&2
  exit 1
fi

if [[ -z "$SUMMARY_PATH" ]]; then
  SUMMARY_PATH="$("$PY_BIN" "$BASE/scripts/runtime_paths.py" live_smoke_summary_path)"
fi
if [[ -z "$LOG_PATH" ]]; then
  LOG_PATH="$("$PY_BIN" "$BASE/scripts/runtime_paths.py" live_smoke_log_path)"
fi

mkdir -p "$(dirname "$SUMMARY_PATH")" "$(dirname "$LOG_PATH")"

python3 - "$RESTING_USD" "$AGGRESSIVE_USD" "$MAX_USD" <<'PY'
import sys
resting = float(sys.argv[1])
aggressive = float(sys.argv[2])
max_usd = float(sys.argv[3])
if resting <= 0 or aggressive <= 0:
    raise SystemExit("resting/aggressive usd must be > 0")
if resting > max_usd or aggressive > max_usd:
    raise SystemExit(f"resting/aggressive usd must be <= {max_usd}")
PY

START_TS="$(python3 - <<'PY'
import time
print(int(time.time()))
PY
)"

set +e
{
  echo "==> live smoke preflight"
  PYTHONPATH=src "$PY_BIN" "$BASE/scripts/live_smoke_preflight.py"
} 2>&1 | tee "$LOG_PATH"
PRE_RC=${PIPESTATUS[0]}

RC=$PRE_RC
if [[ $PRE_RC -eq 0 ]]; then
  {
    echo "==> executing live clob smoke"
    PYTHONPATH=src "$PY_BIN" "$BASE/scripts/live_clob_type2_smoke.py" \
      --token-id "$TOKEN_ID" \
      --resting-usd "$RESTING_USD" \
      --aggressive-usd "$AGGRESSIVE_USD" \
      --max-usd "$MAX_USD" \
      --sleep-seconds "$SLEEP_SECONDS" \
      --yes-live
  } 2>&1 | tee -a "$LOG_PATH"
  RC=${PIPESTATUS[0]}
fi
set -e

python3 - "$SUMMARY_PATH" "$LOG_PATH" "$TOKEN_ID" "$RESTING_USD" "$AGGRESSIVE_USD" "$SLEEP_SECONDS" "$MAX_USD" "$START_TS" "$RC" <<'PY'
import json
import sys
import time
from pathlib import Path

summary_path = Path(sys.argv[1]).expanduser()
log_path = Path(sys.argv[2]).expanduser()
token_id = str(sys.argv[3])
resting_usd = float(sys.argv[4])
aggressive_usd = float(sys.argv[5])
sleep_seconds = float(sys.argv[6])
max_usd = float(sys.argv[7])
start_ts = int(float(sys.argv[8]))
returncode = int(sys.argv[9])
summary = {
    "generated_at": int(time.time()),
    "started_at": start_ts,
    "status": "passed" if returncode == 0 else "failed",
    "ok": returncode == 0,
    "returncode": returncode,
    "token_id": token_id,
    "resting_usd": resting_usd,
    "aggressive_usd": aggressive_usd,
    "sleep_seconds": sleep_seconds,
    "max_usd": max_usd,
    "log_path": str(log_path),
}
summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
print(summary_path)
PY

exit "$RC"
