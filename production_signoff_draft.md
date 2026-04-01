# Polymarket Bot 上线会签草稿

## 基本信息

- 上线窗口：待定
- 目标环境：live
- 版本 / commit：34ae688 (34ae68825c62d1cd10421b7195d8e6ba9aafde12)
- 配置 hash：8d4390b7ff9a21edd3c5ad5977e1b92b1df7e139d0b434eeadce6d540e01dd6a
- 钱包地址：待人工复核
- funder 地址：0x3E8d50d5E0fFda60D14649540fc5429d25F48c2c

## 准入确认

- `make env-check`：通过（有 optional warning，且提示当前 live 无远程告警配置）
- `make one-click`：通过
- `make verify`：通过
- `make full-validate`：通过（`validation_status=PASS`, `operational_readiness=OBSERVE`）
- `make fault-drill`：通过
- `pause_opening=true` 下 30m 观察：已进入观察态，但仍需继续完整窗口
- `DRY_RUN` 长跑观察：未完成
- live connectivity dry-run：未完成
- 告警链路：代码已就绪，生产凭据未配置

## 风险确认

- 当前 `trading_mode`：REDUCE_ONLY
- 当前 `reconciliation.status`：ok
- 当前 `persistence.status`：ok
- 当前 `open_positions`：0
- 当前 `tracked_notional_usd`：0.0
- 当前 `daily_loss_used_pct`：0.0

## 责任人

- 上线批准人：待填写
- 执行人：待填写
- 主值守：待填写
- 备值守：待填写
- 异常升级联系人：待填写

## 会签结论

- 结论：仅观察不放量
- 附加条件：补齐远程告警凭据、完成 24h DRY_RUN、完成 live connectivity dry-run
- 回退条件：`trading_mode != NORMAL/REDUCE_ONLY(仅 operator_pause_opening)`、`reconciliation != ok`、`persistence != ok`、保护链任一 drill 失败

## 签字

- 批准人签字：
- 执行人签字：
- 时间：2026-03-20 16:52:09 CST
