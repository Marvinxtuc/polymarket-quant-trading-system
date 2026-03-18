#!/usr/bin/env bash
set -euo pipefail

BASE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LAUNCHCTL_BIN="/bin/launchctl"
LAUNCHD_DIR="$HOME/Library/LaunchAgents"
LABEL="com.poly.market.monitor-reports"
PLIST="$LAUNCHD_DIR/$LABEL.plist"
UID_NUM="$(id -u)"
RUNTIME_DIR="/tmp/poly_monitor_reports"
BUNDLE_DIR="/tmp/poly_monitor_scheduler_bundle"
METHOD_FILE="$RUNTIME_DIR/method"

if [[ -f "$PLIST" ]]; then
  if [[ ! -x "$LAUNCHCTL_BIN" ]]; then
    echo "launchctl not available" >&2
    exit 1
  fi
  "$LAUNCHCTL_BIN" bootout "gui/$UID_NUM/$LABEL" >/dev/null 2>&1 || true
  rm -f "$PLIST"
  rm -rf "$BUNDLE_DIR"
  rm -f "$METHOD_FILE"
  echo "monitor reports launchd removed: $LABEL"
fi

if [[ -f "$METHOD_FILE" ]]; then
  method="$(awk -F= '$1=="mode"{print $2}' "$METHOD_FILE" 2>/dev/null | head -n 1 || true)"
  if [[ "${method:-launchd}" == "nohup" ]]; then
    "$BASE/scripts/stop_monitor_reports.sh" || true
    echo "monitor reports nohup process stopped"
  fi
  rm -f "$METHOD_FILE"
  rm -rf "$BUNDLE_DIR"
  exit 0
fi

echo "monitor reports launchd not found; skipping"
