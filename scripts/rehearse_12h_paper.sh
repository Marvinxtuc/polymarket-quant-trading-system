#!/usr/bin/env bash
set -euo pipefail

BASE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PY_BIN="$BASE/.venv/bin/python"
RUNTIME_DATA=""
STACK_WEB_PORT="${STACK_WEB_PORT:-8787}"
STATE_URL="http://127.0.0.1:${STACK_WEB_PORT}/api/state"
CONTROL_URL="http://127.0.0.1:${STACK_WEB_PORT}/api/control"
OUT_FILE="${1:-}"
WINDOWS="${2:-10}"
INTERVAL="${3:-3600}"
START_TS="$(date -u '+%Y-%m-%d %H:%M:%S UTC')"
START_EPOCH="$(date +%s)"
DAEMON_LOG=""
STATE_API_CHECK_PATH=""
CONTROL_API_CHECK_PATH=""

if [[ ! -x "$PY_BIN" ]]; then
  echo ".venv/bin/python not found, please create venv first" >&2
  exit 1
fi

eval "$("$PY_BIN" "$BASE/scripts/runtime_paths.py" --format shell runtime_dir bot_log_path rehearsal_10h_out_path state_api_check_path control_api_check_path)"
RUNTIME_DATA="$RUNTIME_DIR"
DAEMON_LOG="$BOT_LOG_PATH"
if [[ -z "${OUT_FILE:-}" ]]; then
  OUT_FILE="$REHEARSAL_10H_OUT_PATH"
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

STATE_URL="$(append_control_token "$STATE_URL")"
CONTROL_URL="$(append_control_token "$CONTROL_URL")"

mkdir -p "$(dirname "$OUT_FILE")"

: > "$OUT_FILE"

record_line() {
  printf '%s\n' "$1" | tee -a "$OUT_FILE"
}

record_line "# ${WINDOWS}h paper rehearsal"
record_line "start=${START_TS}"
record_line "window_hours=${WINDOWS} interval_seconds=${INTERVAL}"
record_line "state_file=$RUNTIME_DATA/state.json"
record_line "checkpoint ts age_seconds state_age_seconds open_positions max_open_positions slot_utilization_pct tracked_notional_usd available_notional_usd daily_loss_used_pct open_position_ratio cooldown_skips_last_window time_exit_close_last_window pause_opening reduce_only emergency_stop report"

if [[ -f "$DAEMON_LOG" ]]; then
  log_offset="$(wc -c < "$DAEMON_LOG")"
else
  log_offset=0
fi

parse_state() {
  local state_file="$STATE_API_CHECK_PATH"
  local control_file="$CONTROL_API_CHECK_PATH"
  local local_state_file="$RUNTIME_DATA/state.json"

  if ! curl -fsS "$STATE_URL" > "$state_file"; then
    if [[ -f "$local_state_file" ]]; then
      cp "$local_state_file" "$state_file"
    else
      return 1
    fi
  fi

  if ! curl -fsS "$CONTROL_URL" > "$control_file" 2>/dev/null; then
    echo '{"pause_opening":false,"reduce_only":false,"emergency_stop":false}' > "$control_file"
  fi

  local metrics
  metrics="$("$PY_BIN" - "$state_file" "$control_file" <<'PY'
import json,sys
state = json.load(open(sys.argv[1], "r", encoding="utf-8"))
control = json.load(open(sys.argv[2], "r", encoding="utf-8"))

summary = state.get("summary", {}) if isinstance(state.get("summary", {}), dict) else {}
ctrl = state.get("control", {}) if isinstance(state.get("control", {}), dict) else {}
if not ctrl:
    ctrl = control

open_positions = int(summary.get("open_positions", 0))
max_open_positions = int(summary.get("max_open_positions", 0))
slot_util = float(summary.get("slot_utilization_pct", 0.0))
tracked_notional = float(summary.get("tracked_notional_usd", 0.0))
available_notional = float(summary.get("available_notional_usd", 0.0))
dlp = float(summary.get("daily_loss_used_pct", 0.0))
ts = int(state.get("ts", 0))

pause_opening = "1" if bool(ctrl.get("pause_opening")) else "0"
reduce_only = "1" if bool(ctrl.get("reduce_only")) else "0"
emergency_stop = "1" if bool(ctrl.get("emergency_stop")) else "0"

ratio = "NA"
if max_open_positions > 0:
    ratio = f"{(open_positions / max_open_positions):.3f}"

print(open_positions, max_open_positions, f"{slot_util:.4f}", f"{tracked_notional:.4f}", f"{available_notional:.4f}", f"{dlp:.4f}", ts, ratio, pause_opening, reduce_only, emergency_stop)
PY
)"
  echo "$metrics"
}

compute_window_counts() {
  local seg="$1"
  local cooldown
  local time_exit_close
  cooldown="$(printf "%s" "$seg" | rg -c 'SKIP .*reason=token add cooldown' || true)"
  time_exit_close="$(printf "%s" "$seg" | rg -c 'TIME_EXIT_CLOSE' || true)"
  cooldown="${cooldown:-0}"
  time_exit_close="${time_exit_close:-0}"
  echo "$cooldown $time_exit_close"
}

i=1
while (( i <= WINDOWS )); do
  now_epoch="$(date +%s)"
  if ! state_metrics="$(parse_state)"; then
    record_line "checkpoint=${i} $(date '+%Y-%m-%d %H:%M:%S') state_api=no_response"
    if (( i < WINDOWS )); then
      sleep "$INTERVAL"
    fi
    ((i++))
    continue
  fi
  read -r open_positions max_open_positions slot_utilization_pct tracked_notional_usd available_notional_usd daily_loss_used_pct state_ts open_ratio pause_opening reduce_only emergency_stop <<< "$state_metrics"

  if [[ -f "$DAEMON_LOG" ]]; then
    current_size="$(wc -c < "$DAEMON_LOG")"
    seg="$(tail -c +$((log_offset + 1)) "$DAEMON_LOG" 2>/dev/null || true)"
    log_offset="$current_size"
  else
    seg=""
  fi

  read -r cooldown_skips time_exit_close <<< "$(compute_window_counts "$seg")"

  report="pass"
  if [[ "$open_positions" -ge "$max_open_positions" ]] && [[ "$max_open_positions" -gt 0 ]]; then
    report="at_limit"
  fi

  age="$(( now_epoch - START_EPOCH ))"
  state_age="$(( now_epoch - state_ts ))"
  if (( state_age < 0 )); then
    state_age=0
  fi

  record_line "checkpoint${i} $(date '+%Y-%m-%d %H:%M:%S') $age $state_age $open_positions $max_open_positions $slot_utilization_pct $tracked_notional_usd $available_notional_usd $daily_loss_used_pct $open_ratio $cooldown_skips $time_exit_close $pause_opening $reduce_only $emergency_stop $report"
  if (( i < WINDOWS )); then
    sleep "$INTERVAL"
  fi
  ((i++))
done

record_line "rehearsal_done=${START_TS} checkpoints=${WINDOWS}"
