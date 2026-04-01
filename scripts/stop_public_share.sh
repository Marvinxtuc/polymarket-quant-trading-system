#!/bin/bash
set -euo pipefail

RUNTIME_DATA="/tmp/poly_runtime_data"
PID_FILE="$RUNTIME_DATA/cloudflared.pid"

if [[ -f "$PID_FILE" ]]; then
  pid="$(cat "$PID_FILE" 2>/dev/null || true)"
  if [[ -n "${pid:-}" ]] && ps -p "$pid" >/dev/null 2>&1; then
    kill "$pid" >/dev/null 2>&1 || true
  fi
  rm -f "$PID_FILE"
fi

pkill -f "cloudflared tunnel --no-autoupdate --url http://127.0.0.1:8787" >/dev/null 2>&1 || true
pkill -f "ssh -T -o StrictHostKeyChecking=no -o ServerAliveInterval=30 -o ExitOnForwardFailure=yes -R 80:127.0.0.1:8787 nokey@localhost.run -- --output json" >/dev/null 2>&1 || true
pkill -f "ssh -N -tt -o StrictHostKeyChecking=no -o ServerAliveInterval=30 -o ExitOnForwardFailure=yes -R 80:127.0.0.1:8787 nokey@localhost.run" >/dev/null 2>&1 || true
echo "public share stopped"
