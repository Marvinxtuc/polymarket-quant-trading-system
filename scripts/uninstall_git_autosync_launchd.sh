#!/usr/bin/env bash
set -euo pipefail

LAUNCHCTL_BIN="/bin/launchctl"
LAUNCHD_DIR="$HOME/Library/LaunchAgents"
LABEL="com.poly.market.git-autosync"
PLIST="$LAUNCHD_DIR/$LABEL.plist"
UID_NUM="$(id -u)"
RUNTIME_DIR="/tmp/poly_git_autosync"
BUNDLE_DIR="/tmp/poly_git_autosync_bundle"
METHOD_FILE="$RUNTIME_DIR/method"

"$LAUNCHCTL_BIN" bootout "gui/$UID_NUM/$LABEL" >/dev/null 2>&1 || true
rm -f "$PLIST" "$METHOD_FILE"
rm -rf "$BUNDLE_DIR"
echo "git autosync launchd removed: $LABEL"
