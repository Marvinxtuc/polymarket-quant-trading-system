#!/usr/bin/env bash
set -euo pipefail

PROJECT_BASE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET_DIR="${POLY_DESKTOP_DIR:-$HOME/Desktop}"
if [[ ! -d "$TARGET_DIR" ]]; then
  TARGET_DIR="$HOME"
fi
TARGET="$TARGET_DIR/一键 poly.command"
LAUNCHER_GUI="$PROJECT_BASE/scripts/run_one_click_launcher_gui.sh"

if [[ ! -x "$LAUNCHER_GUI" ]]; then
  echo "launcher gui missing: $LAUNCHER_GUI"
  exit 1
fi

mkdir -p "$TARGET_DIR"
cat > "$TARGET" <<EOF
#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="${PROJECT_BASE}"
if [[ ! -x "$PROJECT_DIR/scripts/run_one_click_launcher_gui.sh" ]]; then
  echo "launcher missing: $PROJECT_DIR/scripts/run_one_click_launcher_gui.sh"
  exit 1
fi

POLY_ONE_CLICK_FALLBACK="${POLY_ONE_CLICK_FALLBACK:-1}" \
POLY_SKIP_NETWORK_SMOKE="${POLY_SKIP_NETWORK_SMOKE:-1}" \
bash "$PROJECT_DIR/scripts/run_one_click_launcher_gui.sh"
EOF
chmod +x "$TARGET"

echo "desktop_command=$TARGET"
echo "linked_gui_launcher=$LAUNCHER_GUI"
