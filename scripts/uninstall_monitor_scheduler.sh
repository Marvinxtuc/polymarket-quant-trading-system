#!/usr/bin/env bash
set -euo pipefail

BASE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PY_BIN="$BASE/.venv/bin/python"
LAUNCHCTL_BIN="/bin/launchctl"
LAUNCHD_DIR="$HOME/Library/LaunchAgents"
UID_NUM="$(id -u)"
LABEL=""
PLIST=""
RUNTIME_DIR=""
METHOD_FILE=""
LEGACY_LABEL="com.poly.market.monitor-reports"
LEGACY_PLIST="$LAUNCHD_DIR/$LEGACY_LABEL.plist"
LEGACY_RUNTIME_DIR="/tmp/poly_monitor_reports"
LEGACY_BUNDLE_DIR="/tmp/poly_monitor_scheduler_bundle"

resolve_runtime_paths() {
  local runtime_py="$PY_BIN"
  if [[ ! -x "$runtime_py" ]]; then
    runtime_py="$(command -v python3)"
  fi
  eval "$("$runtime_py" "$BASE/scripts/runtime_paths.py" --format shell runtime_dir monitor_reports_dir)"
  local mode identity
  mode="$(basename "$(dirname "$RUNTIME_DIR")")"
  identity="$(basename "$RUNTIME_DIR")"
  LABEL="com.poly.market.monitor-reports.${mode}.${identity}"
  PLIST="$LAUNCHD_DIR/$LABEL.plist"
  RUNTIME_DIR="$MONITOR_REPORTS_DIR"
  METHOD_FILE="$RUNTIME_DIR/method"
}

resolve_runtime_paths

if [[ -f "$PLIST" ]]; then
  if [[ ! -x "$LAUNCHCTL_BIN" ]]; then
    echo "launchctl not available" >&2
    exit 1
  fi
  "$LAUNCHCTL_BIN" bootout "gui/$UID_NUM/$LABEL" >/dev/null 2>&1 || true
  rm -f "$PLIST"
  rm -f "$METHOD_FILE"
  echo "monitor reports launchd removed: $LABEL"
fi

if [[ -f "$LEGACY_PLIST" ]]; then
  if [[ ! -x "$LAUNCHCTL_BIN" ]]; then
    echo "launchctl not available" >&2
    exit 1
  fi
  "$LAUNCHCTL_BIN" bootout "gui/$UID_NUM/$LEGACY_LABEL" >/dev/null 2>&1 || true
  rm -f "$LEGACY_PLIST"
  rm -rf "$LEGACY_BUNDLE_DIR"
  rm -f "$LEGACY_RUNTIME_DIR/method"
  echo "legacy monitor reports launchd removed: $LEGACY_LABEL"
fi

if [[ -f "$METHOD_FILE" ]]; then
  method="$(awk -F= '$1=="mode"{print $2}' "$METHOD_FILE" 2>/dev/null | head -n 1 || true)"
  if [[ "${method:-launchd}" == "nohup" ]]; then
    "$BASE/scripts/stop_monitor_reports.sh" || true
    echo "monitor reports nohup process stopped"
  fi
  rm -f "$METHOD_FILE"
  exit 0
fi

echo "monitor reports launchd not found; skipping"
