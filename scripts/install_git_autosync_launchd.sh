#!/usr/bin/env bash
set -euo pipefail

BASE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LAUNCHCTL_BIN="/bin/launchctl"
LAUNCHD_DIR="$HOME/Library/LaunchAgents"
LABEL="com.poly.market.git-autosync"
PLIST="$LAUNCHD_DIR/$LABEL.plist"
UID_NUM="$(id -u)"
RUNTIME_DIR="/tmp/poly_git_autosync"
BUNDLE_DIR="/tmp/poly_git_autosync_bundle"
METHOD_FILE="$RUNTIME_DIR/method"
START_TS="$(date '+%Y-%m-%d %H:%M:%S %Z')"
PYTHON_BIN="${GIT_AUTOSYNC_PYTHON:-$BASE/.venv/bin/python}"

if [[ ! -x "$LAUNCHCTL_BIN" ]]; then
  echo "launchctl not available" >&2
  exit 1
fi

if [[ ! -x "$PYTHON_BIN" ]]; then
  PYTHON_BIN="$(command -v python3)"
fi

if [[ "${GIT_AUTOSYNC_ALLOW_DIRTY_START:-0}" != "1" ]]; then
  if [[ -n "$(git -C "$BASE" status --porcelain)" ]]; then
    echo "refusing to install git autosync on a dirty worktree." >&2
    echo "commit/stash first, or rerun with GIT_AUTOSYNC_ALLOW_DIRTY_START=1." >&2
    exit 1
  fi
fi

mkdir -p "$RUNTIME_DIR" "$LAUNCHD_DIR" "$BUNDLE_DIR/scripts"
cp "$BASE/scripts/git_autosync.py" "$BUNDLE_DIR/scripts/git_autosync.py"
chmod +x "$BUNDLE_DIR/scripts/git_autosync.py"

cat > "$PLIST" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>$LABEL</string>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><true/>
  <key>WorkingDirectory</key><string>$BASE</string>
  <key>ProgramArguments</key>
  <array>
    <string>$PYTHON_BIN</string>
    <string>$BUNDLE_DIR/scripts/git_autosync.py</string>
    <string>--repo</string>
    <string>$BASE</string>
    <string>--status-path</string>
    <string>$RUNTIME_DIR/status.json</string>
  </array>
  <key>EnvironmentVariables</key>
  <dict>
    <key>PATH</key>
    <string>/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin</string>
    <key>GIT_AUTOSYNC_REMOTE</key>
    <string>${GIT_AUTOSYNC_REMOTE:-origin}</string>
    <key>GIT_AUTOSYNC_POLL_SECONDS</key>
    <string>${GIT_AUTOSYNC_POLL_SECONDS:-2}</string>
    <key>GIT_AUTOSYNC_DEBOUNCE_SECONDS</key>
    <string>${GIT_AUTOSYNC_DEBOUNCE_SECONDS:-3}</string>
    <key>GIT_AUTOSYNC_COMMIT_PREFIX</key>
    <string>${GIT_AUTOSYNC_COMMIT_PREFIX:-auto: sync}</string>
  </dict>
  <key>StandardOutPath</key><string>$RUNTIME_DIR/git-autosync-stdout.log</string>
  <key>StandardErrorPath</key><string>$RUNTIME_DIR/git-autosync-stderr.log</string>
</dict>
</plist>
EOF

"$LAUNCHCTL_BIN" bootout "gui/$UID_NUM/$LABEL" >/dev/null 2>&1 || true
"$LAUNCHCTL_BIN" bootstrap "gui/$UID_NUM" "$PLIST" >/dev/null
"$LAUNCHCTL_BIN" kickstart -k "gui/$UID_NUM/$LABEL" >/dev/null

cat > "$METHOD_FILE" <<EOF
mode=launchd
started=$START_TS
status_file=$RUNTIME_DIR/status.json
EOF

echo "git autosync launchd installed: $LABEL"
echo "plist=$PLIST"
