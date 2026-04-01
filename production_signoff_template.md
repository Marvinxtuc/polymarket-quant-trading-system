# Polymarket Bot 上线会签模板

## 基本信息

- 上线窗口：
- 目标环境：
- 版本 / commit：
- 配置 hash：
- 钱包地址：
- funder 地址：

## 准入确认

- `make env-check`：通过 / 未通过
- `make one-click`：通过 / 未通过
- `make verify`：通过 / 未通过
- `make full-validate`：通过 / 未通过
- `make fault-drill`：通过 / 未通过
- `make monitor-scheduler-smoke`：通过 / 未通过
- `pause_opening=true` 下 30m 观察：完成 / 未完成
- `DRY_RUN` 长跑观察：完成 / 未完成
- live connectivity dry-run：完成 / 未完成
- 告警链路：就绪 / 未就绪

## 证据路径

- `full_flow_validation_report.json`：
- `fault_drill` 输出：
- `monitor_scheduler_smoke` 输出：
- `DRY_RUN rehearsal` 输出：
- `readiness snapshot`：

## 风险确认

- 当前 `trading_mode`：
- 当前 `reconciliation.status`：
- 当前 `persistence.status`：
- 当前 `open_positions`：
- 当前 `tracked_notional_usd`：
- 当前 `daily_loss_used_pct`：

## 责任人

- 上线批准人：
- 执行人：
- 主值守：
- 备值守：
- 异常升级联系人：

## 会签结论

- 结论：允许灰度 / 仅观察不放量 / 禁止上线
- 附加条件：
- 回退条件：

## 签字

- 批准人签字：
- 执行人签字：
- 时间：
