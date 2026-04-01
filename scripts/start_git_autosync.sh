#!/usr/bin/env bash
set -euo pipefail

BASE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUNTIME_DIR="/tmp/poly_git_autosync"
PID_FILE="$RUNTIME_DIR/git-autosync.pid"
STATUS_FILE="$RUNTIME_DIR/status.json"
LOG_FILE="$RUNTIME_DIR/git-autosync.log"
METHOD_FILE="$RUNTIME_DIR/method"
PYTHON_BIN="${GIT_AUTOSYNC_PYTHON:-$BASE/.venv/bin/python}"
START_TS="$(date '+%Y-%m-%d %H:%M:%S %Z')"

mkdir -p "$RUNTIME_DIR"

if [[ ! -x "$PYTHON_BIN" ]]; then
  PYTHON_BIN="$(command -v python3)"
fi

if [[ -f "$PID_FILE" ]]; then
  existing_pid="$(cat "$PID_FILE" 2>/dev/null || true)"
  if [[ -n "${existing_pid:-}" ]] && kill -0 "$existing_pid" >/dev/null 2>&1; then
    echo "git autosync already running: pid=$existing_pid"
    exit 0
  fi
  rm -f "$PID_FILE"
fi

if ! git -C "$BASE" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  echo "not a git repository: $BASE" >&2
  exit 1
fi

if [[ "${GIT_AUTOSYNC_ALLOW_DIRTY_START:-0}" != "1" ]]; then
  if [[ -n "$(git -C "$BASE" status --porcelain)" ]]; then
    echo "refusing to start git autosync on a dirty worktree." >&2
    echo "commit/stash first, or run with GIT_AUTOSYNC_ALLOW_DIRTY_START=1 if you intentionally want the current dirty state auto-pushed." >&2
    exit 1
  fi
fi

nohup "$PYTHON_BIN" "$BASE/scripts/git_autosync.py" \
  --repo "$BASE" \
  --status-path "$STATUS_FILE" \
  > "$LOG_FILE" 2>&1 < /dev/null &

pid=$!
echo "$pid" > "$PID_FILE"
cat > "$METHOD_FILE" <<EOF
mode=nohup
started=$START_TS
pid=$pid
status_file=$STATUS_FILE
log_file=$LOG_FILE
EOF

echo "git autosync started: pid=$pid"
echo "status=$STATUS_FILE"
echo "log=$LOG_FILE"
