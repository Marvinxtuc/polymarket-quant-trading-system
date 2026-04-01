# Polymarket Bot 值守交接草案

## 班次信息

- 交班时间：2026-03-20 19:12:06 CST
- 接班时间：尚未安排，待真实放行窗口确认
- 交班人：Codex
- 接班人：尚未指定，需人工确认

## 当前状态

- `decision_mode`：`manual`
- `pause_opening`：`false`
- `reduce_only`：`false`
- `emergency_stop`：`false`
- `trading_mode`：`NORMAL`
- `reconciliation.status`：`ok`
- `persistence.status`：`ok`
- `open_positions`：`0`
- `tracked_notional_usd`：`0.0`
- 告警链路状态：已接通真实 Telegram 告警
- 当前 rehearsal / 验证状态：`24h dry-run rehearsal` 进行中，当前已通过至 `checkpoint15 ... pass`

## 本班关键事件

- 事件 1：启动真正的 `24h dry-run rehearsal`
- 动作：在长期会话中以前台方式运行 paper stack 与 rehearsal 脚本
- 结果：当前 [24h_dry_run_rehearsal.txt](/tmp/poly_runtime_data/paper/default/24h_dry_run_rehearsal.txt) 已连续写入至 `checkpoint15 ... pass`

- 事件 2：补齐 monitor scheduler stale 检测
- 动作：新增 `make monitor-scheduler-smoke`
- 结果：当前 `nohup -> stale -> reinstall` 演练通过

## 未决风险

- 风险 1：24h rehearsal 尚未完成
- 当前保护状态：paper 模式，未放量
- 需要关注的指标：后续 checkpoint 是否持续 `pass`
- 对应证据路径：
  [24h_dry_run_rehearsal.txt](/tmp/poly_runtime_data/paper/default/24h_dry_run_rehearsal.txt)

- 风险 2：真实 live smoke 仍未执行
- 当前保护状态：`live_smoke_preflight` 已 ready，但还没有真实 smoke 执行摘要
- 需要关注的指标：`live_smoke_execution_summary.json` 是否生成且返回 success
- 对应证据路径：
  [preprod_readiness_snapshot_2026-03-20.md](~/Desktop/Polymarket/preprod_readiness_snapshot_2026-03-20.md)
  [alert_delivery_smoke.json](/tmp/poly_runtime_data/live/0x3e8d50d5e0ffda60d14649540fc5429d25f48c2c/alert_delivery_smoke.json)

- 风险 3：统一 release gate 仍为 `BLOCKED`
- 当前保护状态：所有 blocker 已结构化落盘，当前只剩 rehearsal 和真实 live smoke 两项
- 需要关注的指标：`release_gate_report.json` 中 blocker 数量是否继续下降
- 对应证据路径：
  [live_smoke_preflight.json](/tmp/poly_runtime_data/live/0x3e8d50d5e0ffda60d14649540fc5429d25f48c2c/live_smoke_preflight.json)
  [release_gate_report.json](/tmp/poly_runtime_data/live/0x3e8d50d5e0ffda60d14649540fc5429d25f48c2c/release_gate_report.json)

## 下一班必须做的动作

1. 继续观察 `24h dry-run rehearsal`，确认后续 checkpoint 持续通过。
2. 维持 `live:8788` 的 `manual + pause_opening=true` 安全姿态，等待真实 smoke token 与值守窗口。
3. 用统一入口 `LIVE_SMOKE_ACK=YES LIVE_SMOKE_TOKEN_ID=<token_id> make live-smoke` 执行真实 smoke，并确认 `live_smoke_execution_summary.json` 已生成。
4. rehearsal 完成后先跑 `make rehearsal-finalize`，再重跑 `make release-gate`，直到 `status=READY` 后再考虑放行。

## 升级联系人

- 主联系人：尚未指定，需人工确认
- 备联系人：尚未指定，需人工确认
- 升级条件：
  - rehearsal checkpoint 失败
  - `reconciliation.status != ok`
  - `persistence.status != ok`
  - live connectivity smoke 异常
