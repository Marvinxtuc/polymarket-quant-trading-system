#!/usr/bin/env bash
set -euo pipefail

BASE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PY_BIN="$BASE/.venv/bin/python"
LOG_DIR=""
ROTATE_KEEP="${ROTATE_KEEP:-24}"
PID_FILE=""

MODE="${1:-both}"
MONITOR_DAEMON_LOG="${MONITOR_DAEMON_LOG:-}"
STATE_JSON="${STATE_JSON:-}"

MON30M_OUT="${MON30M_OUT:-}"
MON30M_JSON="${MON30M_JSON:-}"
MON30M_LOG="${MON30M_LOG:-}"
MON30M_WINDOW="${MON30M_WINDOW_SECONDS:-1800}"
MON30M_STATE="${MON30M_STATE:-}"

MON12H_OUT="${MON12H_OUT:-}"
MON12H_JSON="${MON12H_JSON:-}"
MON12H_LOG="${MON12H_LOG:-}"
MON12H_WINDOW="${MON12H_WINDOW_SECONDS:-43200}"
MON12H_STATE="${MON12H_STATE:-}"

if [[ ! -x "$PY_BIN" ]]; then
  echo ".venv/bin/python not found, please create venv first" >&2
  exit 1
fi

eval "$("$PY_BIN" "$BASE/scripts/runtime_paths.py" --format shell monitor_reports_dir bot_log_path state_path monitor_30m_report_path monitor_30m_json_path monitor_30m_state_path monitor_12h_report_path monitor_12h_json_path monitor_12h_state_path)"
LOG_DIR="$MONITOR_REPORTS_DIR"
PID_FILE="$LOG_DIR/run_monitor_reports.pid"
if [[ -z "${MONITOR_DAEMON_LOG:-}" ]]; then
  MONITOR_DAEMON_LOG="$BOT_LOG_PATH"
fi
if [[ -z "${STATE_JSON:-}" ]]; then
  STATE_JSON="$STATE_PATH"
fi
if [[ -z "${MON30M_OUT:-}" ]]; then
  MON30M_OUT="$MONITOR_30M_REPORT_PATH"
fi
if [[ -z "${MON30M_JSON:-}" ]]; then
  MON30M_JSON="$MONITOR_30M_JSON_PATH"
fi
if [[ -z "${MON30M_STATE:-}" ]]; then
  MON30M_STATE="$MONITOR_30M_STATE_PATH"
fi
if [[ -z "${MON12H_OUT:-}" ]]; then
  MON12H_OUT="$MONITOR_12H_REPORT_PATH"
fi
if [[ -z "${MON12H_JSON:-}" ]]; then
  MON12H_JSON="$MONITOR_12H_JSON_PATH"
fi
if [[ -z "${MON12H_STATE:-}" ]]; then
  MON12H_STATE="$MONITOR_12H_STATE_PATH"
fi
MON30M_LOG="${MON30M_LOG:-$MONITOR_DAEMON_LOG}"
MON12H_LOG="${MON12H_LOG:-$MONITOR_DAEMON_LOG}"

mkdir -p "$LOG_DIR"

acquire_pidfile() {
  if [[ -f "$PID_FILE" ]]; then
    local old_pid
    old_pid="$(cat "$PID_FILE" 2>/dev/null || true)"
    if [[ -n "${old_pid:-}" ]] && ps -p "$old_pid" >/dev/null 2>&1; then
      echo "monitor report scheduler already running (pid=$old_pid)"
      exit 0
    fi
    rm -f "$PID_FILE"
  fi

  echo "$$" > "$PID_FILE"
}

cleanup_pidfile() {
  if [[ -f "$PID_FILE" && "$(cat "$PID_FILE" 2>/dev/null || true)" == "$$" ]]; then
    rm -f "$PID_FILE"
  fi
}

trap cleanup_pidfile EXIT INT TERM

run_cycle() {
  local tag="$1"
  local out_log="$2"
  shift 2
  local cmd=("$@")
  local ts
  ts="$(date '+%Y%m%d_%H%M%S')"
  printf '[%s] start mode=%s\n' "$ts" "$tag" | tee -a "$out_log"
  if ! "${cmd[@]}" >>"$out_log" 2>&1; then
    printf '[%s] mode=%s failed\n' "$(date '+%Y%m%d_%H%M%S')" "$tag" | tee -a "$out_log" >&2
  fi
  printf '[%s] done mode=%s\n' "$(date '+%Y%m%d_%H%M%S')" "$tag" | tee -a "$out_log"
}

rotate_logs() {
  local pattern="$1"
  local dir="$LOG_DIR"
  local keep="$ROTATE_KEEP"
  local files
  local file

  files="$(for file in "$dir"/$pattern; do
    if [[ -e "$file" ]]; then
      echo "$(basename "$file")"
    fi
  done | sort -r)"
  local idx=0
  while IFS= read -r f; do
    idx=$((idx + 1))
    if [[ "$idx" -gt "$keep" ]]; then
      rm -f "$dir/$f"
    fi
  done <<< "$files"
}

loop_30m() {
  local log="$LOG_DIR/monitor-30m-$(date '+%Y%m%d_%H%M%S').log"
  run_cycle \
    "30m" \
    "$log" \
    "$BASE/scripts/monitor_thresholds_30m.sh" \
    "$MON30M_OUT" \
    "$MON30M_LOG" \
    "$MON30M_WINDOW" \
    "$MON30M_STATE" \
    "$STATE_JSON" \
    "$MON30M_JSON"
  rotate_logs "monitor-30m-*.log"
}

loop_12h() {
  local log="$LOG_DIR/monitor-12h-$(date '+%Y%m%d_%H%M%S').log"
  run_cycle \
    "12h" \
    "$log" \
    "$BASE/scripts/monitor_thresholds_12h.sh" \
    "$MON12H_OUT" \
    "$MON12H_LOG" \
    "$MON12H_WINDOW" \
    "$MON12H_STATE" \
    "$STATE_JSON" \
    "$MON12H_JSON"
  rotate_logs "monitor-12h-*.log"
}

monitor_30m_forever() {
  while true; do
    loop_30m
  done
}

monitor_12h_forever() {
  while true; do
    loop_12h
  done
}

acquire_pidfile

case "$MODE" in
  30m|monitor-30m)
    monitor_30m_forever
    ;;
  12h|monitor-12h)
    monitor_12h_forever
    ;;
  both|all|*)
    monitor_30m_forever &
    monitor_12h_forever &
    wait
    ;;
esac
