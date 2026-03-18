#!/usr/bin/env bash
set -euo pipefail

RUNTIME_DIR="/tmp/poly_git_autosync"
PID_FILE="$RUNTIME_DIR/git-autosync.pid"
METHOD_FILE="$RUNTIME_DIR/method"

if [[ ! -f "$PID_FILE" ]]; then
  echo "git autosync not running"
  rm -f "$METHOD_FILE"
  exit 0
fi

pid="$(cat "$PID_FILE" 2>/dev/null || true)"
if [[ -n "${pid:-}" ]] && kill -0 "$pid" >/dev/null 2>&1; then
  kill "$pid" >/dev/null 2>&1 || true
  sleep 1
  if kill -0 "$pid" >/dev/null 2>&1; then
    kill -9 "$pid" >/dev/null 2>&1 || true
  fi
fi

rm -f "$PID_FILE" "$METHOD_FILE"
echo "git autosync stopped"
