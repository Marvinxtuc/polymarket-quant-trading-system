# Polymarket Bot 值守交接草稿（2026-04-17）

## 班次信息

- 交班时间：2026-04-17 11:13:39 CST
- 接班时间：下一值守窗口
- 交班人：Codex
- 接班人：未指派

## 当前状态

- `decision_mode`：`manual`
- `pause_opening`：`true`
- `reduce_only`：`false`
- `emergency_stop`：`false`
- `trading_mode`：`REDUCE_ONLY`（`["pause_opening"]`）
- `reconciliation.status`：`ok`
- `persistence.status`：`ok`
- `open_positions`：`0`
- `tracked_notional_usd`：`0.0`
- 告警链路状态：`alert_delivery_smoke.status=sent`
- 当前 rehearsal / 验证状态：24h dry-run 完成；live smoke preflight 阻断（余额不足）

## 本班关键事件

- 事件 1：完成阶段 1 代码并入 `main`（PR #6）
- 动作：提交、推送、建 PR、合并
- 结果：`origin/main` 包含 preflight 资金/授权前置门禁

- 事件 2：修复运行态阻塞
- 动作：拉起本地 signer、刷新 stack、执行 `make network-smoke`
- 结果：`state_fresh/startup_ready/reconciliation_ok` 恢复

- 事件 3：执行全量实盘演练入口
- 动作：执行 `make live-smoke-preflight` 与 `make live-smoke`
- 结果：统一阻断到 `collateralBalanceAllowanceInsufficient`，未触发真实下单

- 事件 4：执行控制链路演练（pause/reduce/emergency）
- 动作：调用 `/api/control` 写入命令并抓取前后状态快照
- 结果：写入统一返回 `write api disabled`，仅保留读链路与故障证据

## 未决风险

- 风险 1：funder collateral 余额为 0，无法通过 smoke 预算校验
- 当前保护状态：`pause_opening=true`，系统处于 `REDUCE_ONLY`
- 需要关注的指标：`live_smoke_preflight.checks[].details.balance_usd`
- 对应证据路径：`/Users/marvin.x/.local/share/poly_runtime_data/live/0x3e8d50d5e0ffda60d14649540fc5429d25f48c2c/live_smoke_preflight.json`

- 风险 2：release gate 仍 blocked
- 当前保护状态：仅观察不放量
- 需要关注的指标：`release_gate_report.blockers`
- 对应证据路径：`/Users/marvin.x/.local/share/poly_runtime_data/live/0x3e8d50d5e0ffda60d14649540fc5429d25f48c2c/release_gate_report.json`

- 风险 3：控制写平面未放开（`write api disabled`）
- 当前保护状态：依赖既有保护位，不可执行控制写演练闭环
- 需要关注的指标：`control_drill_2026-04-17.log` 中所有 POST 结果
- 对应证据路径：`/Users/marvin.x/.local/share/poly_runtime_data/live/0x3e8d50d5e0ffda60d14649540fc5429d25f48c2c/control_drill_2026-04-17.log`

## 下一班必须做的动作

1. 资金侧完成注资并确认 `balance_usd >= 2.0`
2. 重新执行 `LIVE_SMOKE_ACK=YES LIVE_SMOKE_TOKEN_ID=<id> make live-smoke`
3. 放开控制写平面后重做 kill-switch 写演练，再跑 `make release-gate` 与 `make readiness-brief`

## 升级联系人

- 主联系人：未指派
- 备联系人：未指派
- 升级条件：`release_gate` 出现新增 blocker，或 `reconciliation/persistence` 非 `ok`
