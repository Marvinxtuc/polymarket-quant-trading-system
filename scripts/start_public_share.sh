#!/bin/bash
set -euo pipefail

BASE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUNTIME_DATA="/tmp/poly_runtime_data"
PID_FILE="$RUNTIME_DATA/cloudflared.pid"
LOG_FILE="$RUNTIME_DATA/cloudflared.log"
URL_FILE="$RUNTIME_DATA/cloudflared_url.txt"
LOCAL_URL="http://127.0.0.1:8787"
STATE_URL="$LOCAL_URL/api/state"

mkdir -p "$RUNTIME_DATA"

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

write_dotenv_var() {
  local key="$1"
  local value="$2"
  local dotenv="$BASE/.env"
  python3 - <<'PY' "$dotenv" "$key" "$value"
from pathlib import Path
import sys

path = Path(sys.argv[1])
key = sys.argv[2]
value = sys.argv[3]
lines = []
found = False
if path.exists():
    lines = path.read_text(encoding="utf-8").splitlines()
updated = []
for line in lines:
    stripped = line.strip()
    if stripped.startswith(f"{key}="):
        updated.append(f"{key}={value}")
        found = True
    else:
        updated.append(line)
if not found:
    if updated and updated[-1] != "":
        updated.append("")
    updated.append(f"{key}={value}")
path.write_text("\n".join(updated) + "\n", encoding="utf-8")
PY
}

if [[ -f "$PID_FILE" ]]; then
  old_pid="$(cat "$PID_FILE" 2>/dev/null || true)"
  if [[ -n "${old_pid:-}" ]] && ps -p "$old_pid" >/dev/null 2>&1; then
    kill "$old_pid" >/dev/null 2>&1 || true
    sleep 1
  fi
  rm -f "$PID_FILE"
fi

if ! command -v cloudflared >/dev/null 2>&1; then
  echo "cloudflared is not installed; will try localhost.run fallback" >&2
fi

CONTROL_TOKEN="$(read_dotenv_var POLY_CONTROL_TOKEN)"
if [[ -z "${CONTROL_TOKEN:-}" ]]; then
  CONTROL_TOKEN="$(python3 - <<'PY'
import secrets
print(secrets.token_urlsafe(24))
PY
)"
  write_dotenv_var POLY_CONTROL_TOKEN "$CONTROL_TOKEN"
fi

if ! curl -fsS --max-time 3 "${STATE_URL}?token=${CONTROL_TOKEN}" >/dev/null 2>&1; then
  echo "local dashboard is not reachable with current token; restart stack first" >&2
  exit 1
fi

probe_public_url() {
  local base_url="$1"
  local code=""
  code="$(curl -s -L -o /dev/null -w '%{http_code}' "${base_url}/api/state?token=${CONTROL_TOKEN}" || true)"
  [[ "$code" == "200" ]]
}

start_cloudflared() {
  command -v cloudflared >/dev/null 2>&1 || return 1
  : >"$LOG_FILE"
  nohup cloudflared tunnel --no-autoupdate --url "$LOCAL_URL" >"$LOG_FILE" 2>&1 < /dev/null &
  local pid=$!
  echo "$pid" >"$PID_FILE"
  local public_url=""
  for _ in {1..45}; do
    public_url="$(python3 - <<'PY' "$LOG_FILE"
from pathlib import Path
import re
import sys
text = Path(sys.argv[1]).read_text(encoding="utf-8", errors="ignore")
matches = re.findall(r"https://[-a-z0-9]+\.trycloudflare\.com", text)
print(matches[-1] if matches else "")
PY
)"
    if [[ -n "${public_url:-}" ]] && probe_public_url "$public_url"; then
      printf '%s\n' "$public_url" >"$URL_FILE"
      return 0
    fi
    sleep 1
  done
  kill "$pid" >/dev/null 2>&1 || true
  wait "$pid" >/dev/null 2>&1 || true
  rm -f "$PID_FILE"
  return 1
}

start_localhost_run() {
  : >"$LOG_FILE"
  nohup ssh -T -o StrictHostKeyChecking=no -o ServerAliveInterval=30 -o ExitOnForwardFailure=yes -R 80:127.0.0.1:8787 nokey@localhost.run -- --output json >"$LOG_FILE" 2>&1 < /dev/null &
  local pid=$!
  echo "$pid" >"$PID_FILE"
  local public_url=""
  for _ in {1..30}; do
    public_url="$(python3 - <<'PY' "$LOG_FILE"
from pathlib import Path
import json
import re
import sys
text = Path(sys.argv[1]).read_text(encoding="utf-8", errors="ignore")
for line in text.splitlines():
    line = line.strip()
    if not line.startswith("{"):
        continue
    try:
        payload = json.loads(line)
    except Exception:
        continue
    address = str(payload.get("address") or payload.get("listen_host") or "").strip()
    if address:
        print(f"https://{address}")
        break
else:
    matches = re.findall(r"https://(?:[a-z0-9-]+\.)+[a-z]{2,}", text)
    print(matches[-1] if matches else "")
PY
)"
    if [[ -n "${public_url:-}" ]] && probe_public_url "$public_url"; then
      printf '%s\n' "$public_url" >"$URL_FILE"
      return 0
    fi
    sleep 1
  done
  kill "$pid" >/dev/null 2>&1 || true
  wait "$pid" >/dev/null 2>&1 || true
  rm -f "$PID_FILE"
  return 1
}

if ! start_localhost_run; then
  if ! start_cloudflared; then
    echo "failed to create a public tunnel via cloudflared or localhost.run" >&2
    [[ -f "$LOG_FILE" ]] && tail -n 80 "$LOG_FILE" >&2
    exit 1
  fi
fi

public_url="$(cat "$URL_FILE")"
echo "${public_url}/?token=${CONTROL_TOKEN}"
