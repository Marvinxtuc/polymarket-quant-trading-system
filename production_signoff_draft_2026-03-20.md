# Polymarket Bot 上线会签草案

## 基本信息

- 上线窗口：尚未安排，需在 `24h rehearsal` 完成后锁定
- 目标环境：生产前灰度准备
- 版本 / commit：`34ae68825c62d1cd10421b7195d8e6ba9aafde12`
- 配置 hash：`2ffa005389fe30f87a3fb4f4282b381ea0be8de6d4a06227dc02ee65fa7ede60`
- 钱包地址：尚未自动确认，需以真实 `live smoke auth` 输出核对
- funder 地址：`0x3E8d50d5E0fFda60D14649540fc5429d25F48c2c`

## 准入确认

- `make env-check`：通过
  - 备注：仅剩 optional key warning，远程 Telegram 告警已配置
- `make one-click`：已在本地验证链路中通过，当前未重跑以避免打断正在进行的 rehearsal
- `make verify`：通过
- `make full-validate`：通过
- `make fault-drill`：通过
- `make monitor-scheduler-smoke`：通过
- `make alert-smoke`：通过（真实 Telegram 告警 smoke 已 sent）
- `make alert-smoke-local`：通过（本地 webhook 端到端实投已再次验证）
- `make live-smoke-preflight`：通过（真实环境前置条件已全部 ready）
- `STACK_WEB_PORT=8788` fresh live preflight：通过，已验证独立 `live:8788` 可在不打断 rehearsal 的前提下产出 fresh state
- 本地 webhook 条件下 live preflight：通过
- `make release-gate`：阻塞（24h rehearsal / 真实 live smoke 未闭环）
- `make rehearsal-finalize`：当前返回 `PENDING`，说明 rehearsal 尚未完成但收尾入口已就绪
- `pause_opening=true` 下 30m 观察：尚未执行，需在最终放行前由值守人完成
- `DRY_RUN` 长跑观察：进行中，当前已通过至 `checkpoint15 ... pass`
- live connectivity dry-run：未执行
- 告警链路：已就绪（Telegram）

## 证据路径

- `full_flow_validation_report.json`：
  [full_flow_validation_report.json](/tmp/poly_runtime_data/live/0x3e8d50d5e0ffda60d14649540fc5429d25f48c2c/full_flow_validation_report.json)
- `fault_drill` 输出：
  `make fault-drill`
- `monitor_scheduler_smoke` 输出：
  `DRY_RUN=true make monitor-scheduler-smoke`
- `alert_smoke` 输出：
  [alert_delivery_smoke.json](/tmp/poly_runtime_data/live/0x3e8d50d5e0ffda60d14649540fc5429d25f48c2c/alert_delivery_smoke.json)
- `alert_smoke_local` 输出：
  [alert_delivery_smoke_local_summary.json](/tmp/poly_runtime_data/live/0x3e8d50d5e0ffda60d14649540fc5429d25f48c2c/alert_delivery_smoke_local_summary.json)
- `live_smoke_preflight` 输出：
  [live_smoke_preflight.json](/tmp/poly_runtime_data/live/0x3e8d50d5e0ffda60d14649540fc5429d25f48c2c/live_smoke_preflight.json)
  [live_smoke_preflight_fresh_state.json](/tmp/poly_runtime_data/live/0x3e8d50d5e0ffda60d14649540fc5429d25f48c2c/live_smoke_preflight_fresh_state.json)
  [live_smoke_preflight_local_webhook_pass.json](/tmp/poly_runtime_data/live/0x3e8d50d5e0ffda60d14649540fc5429d25f48c2c/live_smoke_preflight_local_webhook_pass.json)
- `DRY_RUN rehearsal` 输出：
  [24h_dry_run_rehearsal.txt](/tmp/poly_runtime_data/paper/default/24h_dry_run_rehearsal.txt)
- `readiness snapshot`：
  [preprod_readiness_snapshot_2026-03-20.md](/Users/marvin.xa/Desktop/Polymarket/preprod_readiness_snapshot_2026-03-20.md)

## 风险确认

- 当前 `trading_mode`：`NORMAL`（paper rehearsal）
- 当前 `reconciliation.status`：`ok`
- 当前 `persistence.status`：`ok`
- 当前 `open_positions`：`0`
- 当前 `tracked_notional_usd`：`0.0`
- 当前 `daily_loss_used_pct`：`0.0`

## 责任人

- 上线批准人：尚未指定，需人工确认
- 执行人：当前由 Codex 完成工程侧准备，最终执行人需人工指定
- 主值守：尚未指定，需人工确认
- 备值守：尚未指定，需人工确认
- 异常升级联系人：尚未指定，需人工确认

## 会签结论

- 结论：当前仅适合继续观察，不具备正式上线放量条件
- 附加条件：
  - 完成 `24h dry-run rehearsal`
  - 完成 live connectivity dry-run
  - 填实责任人与升级链
- 回退条件：
  - rehearsal 出现失败 checkpoint
  - `reconciliation.status != ok`
  - `persistence.status != ok`
  - live connectivity smoke 异常

## 签字

- 批准人签字：尚未签署
- 执行人签字：尚未签署
- 时间：待最终会签时填写
