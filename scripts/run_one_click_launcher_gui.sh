#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE="$(cd "$SCRIPT_DIR/.." && pwd)"
LOG="/tmp/poly_launch.log"

printf '【%s】一键 poly 启动（GUI 入口）\n' "$(date '+%Y-%m-%d %H:%M:%S')"

echo "日志: $LOG"
echo "仓库: $BASE"

action_rc=0
POLY_ONE_CLICK_FALLBACK="${POLY_ONE_CLICK_FALLBACK:-1}" \
POLY_SKIP_NETWORK_SMOKE="${POLY_SKIP_NETWORK_SMOKE:-1}" \
bash "$BASE/scripts/run_one_click_launcher.sh" 2>&1 | tee -a "$LOG" || action_rc=$?

if [[ "$action_rc" -eq 0 ]]; then
  echo "启动已触发，3秒后退出（窗口若自动关闭请双击命令脚本）"
  sleep 3
else
  echo "启动失败，退出码=$action_rc。最近日志摘要："
  tail -n 40 "$LOG" | sed -n '1,40p'
  echo "按回车关闭"
  read -r _
fi

exit "$action_rc"
