# Polymarket Bot 值守交接草稿

## 班次信息

- 交班时间：2026-03-20 16:52:09 CST
- 接班时间：待填写
- 交班人：待填写
- 接班人：待填写

## 当前状态

- `decision_mode`：manual
- `pause_opening`：True
- `reduce_only`：False
- `emergency_stop`：False
- `trading_mode`：REDUCE_ONLY ['operator_pause_opening']
- `reconciliation.status`：ok
- `persistence.status`：ok
- `open_positions`：0
- `tracked_notional_usd`：0.0

## 本班关键事件

- 事件 1：控制面整改后的完整验收再次通过
- 动作：执行 `make one-click`、`make verify`、`make full-validate`、`make fault-drill`
- 结果：保护链正常；运营结论仍为 OBSERVE

## 未决风险

- 风险 1：未配置远程告警凭据
- 当前保护状态：本地 notifier/状态面正常，critical alerts 代码已接通
- 需要关注的指标：webhook/telegram 配置完成后需再做一次通知验收

- 风险 2：24h DRY_RUN 未完成
- 当前保护状态：已提供 `make rehearse-24h` 与 `make rehearse-24h-progress`
- 需要关注的指标：24h 内 `trading_mode/reconciliation/persistence` 持续健康

- 风险 3：live connectivity dry-run 未完成
- 当前保护状态：当前保持 `manual + pause_opening=true`
- 需要关注的指标：真实账户连通性、下单/撤单链路、首单前人工复核

## 下一班必须做的动作

1. 配置远程告警凭据并做真人通知验收
2. 启动并持续观察 `make rehearse-24h`
3. 在人工盯盘条件下执行 live connectivity dry-run

## 升级联系人

- 主联系人：待填写
- 备联系人：待填写
- 升级条件：`trading_mode` 非预期、`reconciliation` 非 `ok`、`persistence` 非 `ok`、保护链任一 drill 失败
