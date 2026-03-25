#!/bin/bash
set -euo pipefail

BASE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PY_BIN="$BASE/.venv/bin/python"
CURL_BIN="/usr/bin/curl"
STACK_WEB_PORT="${STACK_WEB_PORT:-8787}"
WEB_URL="${1:-http://127.0.0.1:${STACK_WEB_PORT}/api/state}"
STATE_PATH="${STATE_PATH:-}"
VERIFY_RETRIES="${VERIFY_RETRIES:-8}"
VERIFY_RETRY_INTERVAL="${VERIFY_RETRY_INTERVAL_SECONDS:-5}"
VERIFY_STARTED_AT="${VERIFY_STARTED_AT:-0}"
VERIFY_ALLOW_STATE_FILE="${VERIFY_ALLOW_STATE_FILE:-0}"

cd "$BASE"

if [[ ! -x "$PY_BIN" ]]; then
  echo ".venv/bin/python not found, please create the virtualenv first" >&2
  exit 1
fi

if [[ -z "${STATE_PATH:-}" ]]; then
  STATE_PATH="$("$PY_BIN" "$BASE/scripts/runtime_paths.py" state_path)"
fi

read_dotenv_var() {
  local key="$1"
  local dotenv="$BASE/.env"
  [[ -f "$dotenv" ]] || return 0
  awk -F= -v key="$key" '
    $0 ~ "^[[:space:]]*" key "=" {
      sub(/^[[:space:]]*[^=]+=/, "", $0)
      gsub(/^[[:space:]]+|[[:space:]]+$/, "", $0)
      gsub(/^"|"$/, "", $0)
      gsub(/^'\''|'\''$/, "", $0)
      print $0
      exit
    }
  ' "$dotenv"
}

append_control_token() {
  local url="$1"
  local token="${POLY_CONTROL_TOKEN:-$(read_dotenv_var POLY_CONTROL_TOKEN)}"
  if [[ -z "${token:-}" ]] || [[ "$url" == *"token="* ]]; then
    printf '%s\n' "$url"
    return 0
  fi
  if [[ "$url" == *"?"* ]]; then
    printf '%s&token=%s\n' "$url" "$token"
  else
    printf '%s?token=%s\n' "$url" "$token"
  fi
}

WEB_URL="$(append_control_token "$WEB_URL")"

fetch_state_payload() {
  local payload

  if payload="$($CURL_BIN -fsS --max-time 3 "$WEB_URL" 2>/dev/null)"; then
    echo "source=api"
    printf '%s\n' "$payload"
    return 0
  fi

  if [[ "$VERIFY_ALLOW_STATE_FILE" != "1" ]]; then
    echo "state API unreachable: $WEB_URL" >&2
    return 1
  fi

  if [[ ! -f "$STATE_PATH" ]]; then
    echo "state API unreachable and state file missing: $STATE_PATH" >&2
    return 1
  fi

  echo "source=state-file"
  cat "$STATE_PATH"
}

verify_state_payload() {
  local payload="$1"
  local source_name="$2"
  local status_out
  local code

  status_out="$(
    STATE_PAYLOAD="$payload" \
    STATE_SOURCE="$source_name" \
    VERIFY_STARTED_AT="$VERIFY_STARTED_AT" \
    "$PY_BIN" - <<'PY'
from __future__ import annotations

import json
import os
import time

raw = os.environ.get("STATE_PAYLOAD", "")
source_name = os.environ.get("STATE_SOURCE", "unknown")
verify_started_at = int(os.environ.get("VERIFY_STARTED_AT", "0") or 0)
if not raw:
    raise SystemExit("state payload is empty")

try:
    payload = json.loads(raw)
except json.JSONDecodeError as exc:
    raise SystemExit(f"state payload is not valid JSON: {exc}") from exc

if not isinstance(payload, dict):
    raise SystemExit("state payload is not a JSON object")

config = payload.get("config") or {}
summary = payload.get("summary") or {}
if not isinstance(config, dict) or not isinstance(summary, dict):
    raise SystemExit("state payload is missing config/summary objects")

ts = int(payload.get("ts") or 0)
poll = int(config.get("poll_interval_seconds") or 0)
mode = str(config.get("execution_mode") or ("paper" if config.get("dry_run", True) else "live")).lower()
broker = str(config.get("broker_name") or ("PaperBroker" if mode == "paper" else "LiveClobBroker"))
wallets = int(config.get("wallet_pool_size") or 0)
open_positions = int(summary.get("open_positions") or 0)
max_open_positions = int(summary.get("max_open_positions") or 0)
tracked_notional = float(summary.get("tracked_notional_usd") or 0.0)

if ts <= 0:
    raise SystemExit("state timestamp is missing or zero")
if poll <= 0:
    raise SystemExit("poll_interval_seconds is missing or zero")

age = max(0, int(time.time()) - ts)
max_age = max(90, poll * 3)
if age > max_age:
    if verify_started_at and ts <= verify_started_at:
        raise SystemExit(2)
    raise SystemExit(
        f"state is stale: age={age}s exceeds max_age={max_age}s "
        f"(poll_interval_seconds={poll})"
    )

print(
    "OK: "
    f"source={source_name} "
    f"mode={mode} "
    f"broker={broker} "
    f"poll={poll}s "
    f"age={age}s "
    f"wallets={wallets} "
    f"open={open_positions}/{max_open_positions} "
    f"tracked_notional=${tracked_notional:.2f}"
)
PY
  )"
  code=$?
  if [[ "$code" -ne 0 ]]; then
    if [[ "$code" -eq 2 ]]; then
      return 2
    fi
    echo "$status_out" >&2
    return 1
  fi

  echo "$status_out"
  return 0
}

attempt=1
while true; do
  payload_out="$(fetch_state_payload)" || exit 1
  source_name="$(printf '%s\n' "$payload_out" | head -n1 | sed 's/^source=//')"
  payload="$(printf '%s\n' "$payload_out" | tail -n +2)"

  if [[ -z "${payload:-}" ]]; then
    echo "state payload empty" >&2
    exit 1
  fi

  if verify_state_payload "$payload" "$source_name"; then
    break
  else
    rc=$?
  fi
  if [[ "$rc" -eq 2 ]]; then
    echo "verify_stack: state looks stale and appears older than this run start (source=$source_name)" >&2
  else
    echo "verify_stack: state is stale, retrying ($attempt/$VERIFY_RETRIES)" >&2
  fi

  if [[ "$attempt" -ge "$VERIFY_RETRIES" ]]; then
    case "$rc" in
      2) echo "state is stale and tied to an older process state; please restart the stack and re-run" >&2 ;;
      *) echo "state is stale: age exceeds freshness threshold after $attempt attempts" >&2 ;;
    esac
    exit 1
  fi

  attempt=$((attempt + 1))
  sleep "$VERIFY_RETRY_INTERVAL"
done
