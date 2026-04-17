# Polymarket Bot 上线记录草稿（2026-04-17）

## 发布信息

- 发布开始时间：2026-04-17 09:30:00 CST
- 发布结束时间：2026-04-17 11:13:39 CST
- 环境：live
- 版本 / commit：`8d0416bec677897adc1243c6b77452070aecddf0`（执行期包含未合并工作树修复）
- 配置 hash：`47ab31a2dfac79ee8c08c341914f504c809e23355deb33e2f3c3e3d3e8fea990`
- 钱包地址：`0xe499173596f4b906261d1ef8fd36cc4a75215a5f`（signer identity）
- funder 地址：`0x3E8d50d5E0fFda60D14649540fc5429d25F48c2c`

## 执行记录

1. `make start-stack`：通过（state fresh，API 可达）
2. `make network-smoke`：通过（3 个 warning）
3. `make live-smoke-preflight`：失败（`collateralBalanceAllowanceInsufficient`）
4. `LIVE_SMOKE_ACK=YES LIVE_SMOKE_TOKEN_ID=<id> make live-smoke`：失败（preflight fail-close，未进入下单）
5. `make release-gate`：`blocked`
6. `make readiness-brief`：`release_gate_status=blocked`
7. 控制链路演练资料：已收集（见回退触发表与 API 路径说明）
8. 结论产出：本窗口 NO-GO，进入资金补齐后复测流程

## 关键快照

- `decision_mode`：`manual`
- `pause_opening`：`true`
- `reduce_only`：`false`
- `emergency_stop`：`false`
- `trading_mode.mode`：`REDUCE_ONLY`
- `trading_mode.reason_codes`：`["pause_opening"]`
- `reconciliation.status`：`ok`
- `persistence.status`：`ok`

## 证据路径

- `full_flow_validation_report.json`：`/Users/marvin.x/.local/share/poly_runtime_data/live/0x3e8d50d5e0ffda60d14649540fc5429d25f48c2c/full_flow_validation_report.json`
- `reconciliation_eod_report.json`：`/Users/marvin.x/.local/share/poly_runtime_data/live/0x3e8d50d5e0ffda60d14649540fc5429d25f48c2c/reconciliation_eod_report.json`（本窗口未更新）
- `monitor_30m_report.json`：`/Users/marvin.x/.local/share/poly_runtime_data/live/0x3e8d50d5e0ffda60d14649540fc5429d25f48c2c/monitor_30m_report.json`
- `monitor_12h_report.json`：`/Users/marvin.x/.local/share/poly_runtime_data/live/0x3e8d50d5e0ffda60d14649540fc5429d25f48c2c/monitor_12h_report.json`
- `release_gate_report.json`：`/Users/marvin.x/.local/share/poly_runtime_data/live/0x3e8d50d5e0ffda60d14649540fc5429d25f48c2c/release_gate_report.json`
- `live_smoke_preflight.json`：`/Users/marvin.x/.local/share/poly_runtime_data/live/0x3e8d50d5e0ffda60d14649540fc5429d25f48c2c/live_smoke_preflight.json`
- `live_smoke_execution_summary.json`：`/Users/marvin.x/.local/share/poly_runtime_data/live/0x3e8d50d5e0ffda60d14649540fc5429d25f48c2c/live_smoke_execution_summary.json`
- `readiness_brief.json`：`/Users/marvin.x/.local/share/poly_runtime_data/live/0x3e8d50d5e0ffda60d14649540fc5429d25f48c2c/readiness_brief.json`
- `network_smoke_log`：`/Users/marvin.x/.local/share/poly_runtime_data/live/0x3e8d50d5e0ffda60d14649540fc5429d25f48c2c/poly_network_smoke.jsonl`

## 异常与处理

- 异常 1：preflight collateral 检查阻断（余额不足）
- 处理动作：修复 signer 可用性、刷新 state、新增 sig-type2 preflight 兼容后复测
- 结果：阻断已定位为业务条件不足（`balance_usd=0.0`），非接口不可用

## 发布结论

- 结果：回退到观察态（NO-GO）
- 是否放量：否
- 下一检查点：
  - 资金到账后重跑 `make live-smoke`
  - `make release-gate` 无 blocker 后再会签
