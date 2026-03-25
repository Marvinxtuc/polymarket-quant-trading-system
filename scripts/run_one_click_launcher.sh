#!/usr/bin/env bash
set -eu
set -o pipefail

export PATH="/usr/bin:/bin:/usr/sbin:/sbin:/opt/homebrew/bin:/usr/local/bin"

PROJECT_BASE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PY_BIN="$PROJECT_BASE/.venv/bin/python"
LAUNCH_LOG="/tmp/poly_launch.log"
OPEN_BIN="/usr/bin/open"
OPEN_URL_BASE="http://127.0.0.1:8787"

if [[ -x "$PY_BIN" ]]; then
  LAUNCH_LOG="$("$PY_BIN" "$PROJECT_BASE/scripts/runtime_paths.py" runtime_dir 2>/dev/null | awk 'NR==1{print $0"/launch.log"}')"
fi
mkdir -p "$(dirname "$LAUNCH_LOG")"

log() {
  printf '%s\n' "$*" | tee -a "$LAUNCH_LOG"
}

run_target() {
  local target="$1"
  log "launch_target: $target"
  local rc=0
  if make "$target" 2>&1 | tee -a "$LAUNCH_LOG"; then
    rc=0
  else
    rc=${PIPESTATUS[0]}
  fi
  if [[ "${rc:-0}" -ne 0 ]]; then
    return "$rc"
  fi
  return 0
}

log "==== $(date '+%Y-%m-%d %H:%M:%S') 一键 poly(共用入口) 启动 ===="
cd "$PROJECT_BASE"

if [[ ! -f "Makefile" ]]; then
  log "project missing: $PROJECT_BASE"
  exit 1
fi

RUN_WITH="${POLY_ONE_CLICK_TARGET:-one-click}"
if [[ "$RUN_WITH" == "lite" ]]; then
  RUN_WITH="one-click-lite"
fi
if [[ "${POLY_SKIP_NETWORK_SMOKE:-0}" == "1" && "$RUN_WITH" != "one-click-lite" ]]; then
  RUN_WITH="one-click-lite"
fi

set +e
run_target "$RUN_WITH"
MAKE_RC=$?
set -e
if [[ "$MAKE_RC" -ne 0 ]]; then
  log "launch_failed: make $RUN_WITH exit=$MAKE_RC"

  if [[ "${POLY_ONE_CLICK_FALLBACK:-1}" == "1" && "$RUN_WITH" != "one-click-lite" ]]; then
    log "launch_retry: fallback to one-click-lite"
    set +e
    run_target "one-click-lite"
    MAKE_RC=$?
    set -e
    if [[ "$MAKE_RC" -ne 0 ]]; then
      log "launch_failed: make one-click-lite exit=$MAKE_RC"
      exit "$MAKE_RC"
    fi
  else
    exit "$MAKE_RC"
  fi
fi

sleep 1
TS="$(date +%s)"
if ! "$OPEN_BIN" "${OPEN_URL_BASE}/?v=$TS" >>"$LAUNCH_LOG" 2>&1; then
  log "launch_open_failed: cannot open browser"
fi
log "launch_ok"
exit 0
