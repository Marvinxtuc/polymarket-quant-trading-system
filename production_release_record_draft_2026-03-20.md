# Polymarket Bot 上线记录草案

## 发布信息

- 发布开始时间：尚未开始，等待 `24h rehearsal` 完成与最终放行
- 发布结束时间：尚未结束，等待真实 smoke 与最终会签
- 环境：生产前灰度准备
- 版本 / commit：`34ae68825c62d1cd10421b7195d8e6ba9aafde12`
- 配置 hash：`2ffa005389fe30f87a3fb4f4282b381ea0be8de6d4a06227dc02ee65fa7ede60`
- 钱包地址：尚未自动确认，需以真实 `live smoke auth` 输出核对
- funder 地址：`0x3E8d50d5E0fFda60D14649540fc5429d25F48c2c`

## 执行记录

1. `make env-check`：通过，仅剩 optional key warning
2. `make one-click`：此前本地通过，当前未重跑以避免影响 rehearsal
3. `make verify`：通过
4. `make full-validate`：通过，`operational_readiness=OBSERVE`
5. `make fault-drill`：通过
6. `make monitor-scheduler-smoke`：通过
7. `make alert-smoke`：通过，真实 Telegram 告警 smoke 已 sent
8. `make alert-smoke-local`：通过，已再次验证本地 webhook sink 端到端实投
9. `make live-smoke-preflight`：通过，真实环境前置条件已全部 ready
10. `STACK_WEB_PORT=8788` fresh live preflight：已再次验证 state freshness 可在不打断 rehearsal 的前提下满足，当前独立 `live:8788` 已成功产出 fresh state
11. 控制接口演练：此前已完成 `pause_opening / reduce_only / emergency_stop`
12. monitor / reconciliation 报告：当前 live 报告为观察态，paper rehearsal 已运行至 `checkpoint15 ... pass`
13. `make release-gate`：阻塞，当前仅剩 `24h rehearsal / 真实 live smoke` 两类 blocker
14. `make rehearsal-finalize`：当前返回 `PENDING`，说明 rehearsal 尚未完成，但收尾入口已可用

## 关键快照

- `decision_mode`：`manual`（paper rehearsal 当前控制面）
- `pause_opening`：`false`
- `reduce_only`：`false`
- `emergency_stop`：`false`
- `trading_mode.mode`：`NORMAL`
- `trading_mode.reason_codes`：`[]`
- `reconciliation.status`：`ok`
- `persistence.status`：`ok`

## 证据路径

- `full_flow_validation_report.json`：
  [full_flow_validation_report.json](/tmp/poly_runtime_data/live/0x3e8d50d5e0ffda60d14649540fc5429d25f48c2c/full_flow_validation_report.json)
- `reconciliation_eod_report.json`：
  [reconciliation_eod_report.json](/tmp/poly_runtime_data/live/0x3e8d50d5e0ffda60d14649540fc5429d25f48c2c/reconciliation_eod_report.json)
- `monitor_30m_report.json`：
  [monitor_30m_report.json](/tmp/poly_runtime_data/live/0x3e8d50d5e0ffda60d14649540fc5429d25f48c2c/monitor_30m_report.json)
- `monitor_12h_report.json`：
  [monitor_12h_report.json](/tmp/poly_runtime_data/live/0x3e8d50d5e0ffda60d14649540fc5429d25f48c2c/monitor_12h_report.json)
- `DRY_RUN rehearsal` 输出：
  [24h_dry_run_rehearsal.txt](/tmp/poly_runtime_data/paper/default/24h_dry_run_rehearsal.txt)
- `alert_smoke` 输出：
  [alert_delivery_smoke.json](/tmp/poly_runtime_data/live/0x3e8d50d5e0ffda60d14649540fc5429d25f48c2c/alert_delivery_smoke.json)
- `alert_smoke_local` 输出：
  [alert_delivery_smoke_local_summary.json](/tmp/poly_runtime_data/live/0x3e8d50d5e0ffda60d14649540fc5429d25f48c2c/alert_delivery_smoke_local_summary.json)
- `live_smoke_preflight` 输出：
  [live_smoke_preflight.json](/tmp/poly_runtime_data/live/0x3e8d50d5e0ffda60d14649540fc5429d25f48c2c/live_smoke_preflight.json)
  [live_smoke_preflight_fresh_state.json](/tmp/poly_runtime_data/live/0x3e8d50d5e0ffda60d14649540fc5429d25f48c2c/live_smoke_preflight_fresh_state.json)
  [live_smoke_preflight_local_webhook_pass.json](/tmp/poly_runtime_data/live/0x3e8d50d5e0ffda60d14649540fc5429d25f48c2c/live_smoke_preflight_local_webhook_pass.json)
  [live_smoke_preflight_2026-03-21.json](/tmp/poly_runtime_data/live/0x3e8d50d5e0ffda60d14649540fc5429d25f48c2c/live_smoke_preflight_2026-03-21.json)
  [live_smoke_preflight_2026-03-21_local_webhook.json](/tmp/poly_runtime_data/live/0x3e8d50d5e0ffda60d14649540fc5429d25f48c2c/live_smoke_preflight_2026-03-21_local_webhook.json)
- `readiness snapshot`：
  [preprod_readiness_snapshot_2026-03-20.md](~/Desktop/Polymarket/preprod_readiness_snapshot_2026-03-20.md)
- `release_gate` 输出：
  [release_gate_report.json](/tmp/poly_runtime_data/live/0x3e8d50d5e0ffda60d14649540fc5429d25f48c2c/release_gate_report.json)

## 异常与处理

- 异常 1：真实 live smoke 仍未执行
- 处理动作：技术前置条件和远程告警均已打通，等待真实 smoke token 与值守窗口
- 结果：进行中

- 异常 2：`operational_readiness` 仍为 `OBSERVE`
- 处理动作：继续执行 24h dry-run rehearsal；当前已通过至 `checkpoint15`
- 结果：进行中

- 异常 3：统一 release gate 当前仍为 `BLOCKED`
- 处理动作：按 gate blocker 逐项完成 24h rehearsal 和真实 live smoke；文档占位符已清理为明确状态说明
- 结果：进行中

## 发布结论

- 结果：继续观察
- 是否放量：否
- 下一检查点：24h rehearsal 后续 checkpoint 与远程告警配置完成后
