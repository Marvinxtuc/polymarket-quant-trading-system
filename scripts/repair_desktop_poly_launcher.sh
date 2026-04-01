#!/usr/bin/env bash
set -euo pipefail

PROJECT_BASE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROJECT_DIR="${2:-$PROJECT_BASE}"
TARGET_APP="${1:-$HOME/Desktop/一键 poly.app}"
TARGET_CMD_DIR="${POLY_DESKTOP_DIR:-$HOME/Desktop}"
if [[ ! -d "$TARGET_CMD_DIR" ]]; then
  TARGET_CMD_DIR="$HOME"
fi
DESKTOP_CMD="$TARGET_CMD_DIR/一键 poly.command"
TARGET_STARTER="$TARGET_APP/Contents/MacOS/start_app"
LAUNCHER_GUI="$PROJECT_DIR/scripts/run_one_click_launcher_gui.sh"
LAUNCHER="$PROJECT_DIR/scripts/run_one_click_launcher.sh"

if [[ ! -x "$LAUNCHER_GUI" ]]; then
  echo "launcher gui missing: $LAUNCHER_GUI"
  exit 1
fi

mkdir -p "$(dirname "$DESKTOP_CMD")"

if [[ ! -d "$TARGET_APP/Contents/MacOS" ]]; then
  echo "target app missing: $TARGET_APP/Contents/MacOS"
  exit 1
fi

cat > "$TARGET_STARTER" <<EOF
#!/usr/bin/env bash
set -eu
set -o pipefail

PROJECT_DIR="$PROJECT_DIR"
DESKTOP_CMD="$DESKTOP_CMD"
LAUNCHER_GUI="\$PROJECT_DIR/scripts/run_one_click_launcher_gui.sh"
LAUNCHER="\$PROJECT_DIR/scripts/run_one_click_launcher.sh"

# Default path for double-clicking .app: open Terminal with .command so user sees progress.
if [[ "\${POLY_APP_DIRECT:-0}" != "1" ]] && [[ -x "\$DESKTOP_CMD" ]]; then
  /usr/bin/open -a Terminal "\$DESKTOP_CMD" >/dev/null 2>&1 && exit 0
  APP_CMD="POLY_ONE_CLICK_FALLBACK=\${POLY_ONE_CLICK_FALLBACK:-1} POLY_SKIP_NETWORK_SMOKE=\${POLY_SKIP_NETWORK_SMOKE:-1} bash \"\$DESKTOP_CMD\""
  APP_CMD="\${APP_CMD//\"/\\\\\"}"
  /usr/bin/osascript \
    -e "tell application \"Terminal\" to activate" \
    -e "tell application \"Terminal\" to do script \"\$APP_CMD\"" >/dev/null 2>&1 && exit 0
fi

if [[ -x "\$LAUNCHER_GUI" ]]; then
  POLY_ONE_CLICK_FALLBACK="\${POLY_ONE_CLICK_FALLBACK:-1}" \\
  POLY_SKIP_NETWORK_SMOKE="\${POLY_SKIP_NETWORK_SMOKE:-1}" \\
  bash "\$LAUNCHER_GUI"
else
  POLY_ONE_CLICK_FALLBACK="\${POLY_ONE_CLICK_FALLBACK:-1}" \\
  bash "\$LAUNCHER"
fi
EOF
chmod +x "$TARGET_STARTER"

cat > "$DESKTOP_CMD" <<EOF
#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$PROJECT_DIR"
if [[ ! -x "$PROJECT_DIR/scripts/run_one_click_launcher_gui.sh" ]]; then
  PROJECT_DIR="$PROJECT_BASE"
fi

POLY_ONE_CLICK_FALLBACK="\${POLY_ONE_CLICK_FALLBACK:-1}" \\
POLY_SKIP_NETWORK_SMOKE="\${POLY_SKIP_NETWORK_SMOKE:-1}" \\
bash "$PROJECT_DIR/scripts/run_one_click_launcher_gui.sh"
EOF
chmod +x "$DESKTOP_CMD"

echo "app_refreshed=$TARGET_APP"
echo "start_app=$TARGET_STARTER"
echo "command=$DESKTOP_CMD"
echo "launcher=$LAUNCHER"
