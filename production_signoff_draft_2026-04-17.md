# Polymarket Bot 上线会签草稿（2026-04-17）

## 基本信息

- 上线窗口：2026-04-17 09:30-12:00 CST（实盘放行前收口窗口）
- 目标环境：live
- 版本 / commit：`8d0416bec677897adc1243c6b77452070aecddf0`（执行期包含未合并工作树修复）
- 配置 hash：`47ab31a2dfac79ee8c08c341914f504c809e23355deb33e2f3c3e3d3e8fea990`
- 钱包地址：`0xe499173596f4b906261d1ef8fd36cc4a75215a5f`（signer identity）
- funder 地址：`0x3E8d50d5E0fFda60D14649540fc5429d25F48c2c`

## 准入确认

- `make env-check`：通过（optional warning）
- `make one-click`：本窗口未执行
- `make verify`：本窗口未执行
- `make full-validate`：沿用现有 PASS 报告
- `make fault-drill`：本窗口未执行
- `make monitor-scheduler-smoke`：本窗口未执行
- `pause_opening=true` 下 30m 观察：状态持续有效（`trading_mode=REDUCE_ONLY`）
- `DRY_RUN` 长跑观察：沿用既有 24h rehearsal 结果（PASS）
- live connectivity dry-run：已执行，结果为 preflight fail-close（余额不足）
- 告警链路：远程通道 smoke 已有 sent 记录

## 证据路径

- `full_flow_validation_report.json`：`/Users/marvin.x/.local/share/poly_runtime_data/live/0x3e8d50d5e0ffda60d14649540fc5429d25f48c2c/full_flow_validation_report.json`
- `release_gate_report.json`：`/Users/marvin.x/.local/share/poly_runtime_data/live/0x3e8d50d5e0ffda60d14649540fc5429d25f48c2c/release_gate_report.json`
- `live_smoke_preflight.json`：`/Users/marvin.x/.local/share/poly_runtime_data/live/0x3e8d50d5e0ffda60d14649540fc5429d25f48c2c/live_smoke_preflight.json`
- `live_smoke_execution_summary.json`：`/Users/marvin.x/.local/share/poly_runtime_data/live/0x3e8d50d5e0ffda60d14649540fc5429d25f48c2c/live_smoke_execution_summary.json`
- `readiness_brief.json`：`/Users/marvin.x/.local/share/poly_runtime_data/live/0x3e8d50d5e0ffda60d14649540fc5429d25f48c2c/readiness_brief.json`
- `24h_dry_run_rehearsal.txt`：`/Users/marvin.x/.local/share/poly_runtime_data/paper/default/24h_dry_run_rehearsal.txt`

## 风险确认

- 当前 `trading_mode`：`REDUCE_ONLY`
- 当前 `trading_mode.reason_codes`：`["pause_opening"]`
- 当前 `reconciliation.status`：`ok`
- 当前 `persistence.status`：`ok`
- 当前 `open_positions`：`0`
- 当前 `tracked_notional_usd`：`0.0`
- 当前 `daily_loss_used_pct`：`0.0`
- 当前 release gate：`blocked`（blockers: `live_smoke_preflight`, `live_smoke_execution`）

## 责任人

- 上线批准人：未指派
- 执行人：Codex + Marvin
- 主值守：未指派
- 备值守：未指派
- 异常升级联系人：未指派

## 会签结论

- 结论：禁止上线（NO-GO）
- 附加条件：
  - 资金侧完成注资（collateral `balance >= 2.00 USDC`）
  - 重新执行 `LIVE_SMOKE_ACK=YES LIVE_SMOKE_TOKEN_ID=<id> make live-smoke` 且成功
  - `make release-gate` 清零 blocker（允许仅保留 `operational_readiness=observe` advisory）
  - 控制写平面恢复可用（`/api/control` 不再返回 `write api disabled`）
- 条件放行复核点（仅在上述条件全部满足后生效）：
  - 首次复核：放行后 +30 分钟
  - 二次复核：放行后 +60 分钟
- 回退条件：
  - `reconciliation.status != ok`
  - `persistence.status != ok`
  - `kill_switch.manual_required=true` 或 `phase=FAILED_MANUAL_REQUIRED`
  - `trading_mode.mode=HALTED` 或 `reason_codes` 出现 `kill_switch_cancel_timeout` / `kill_switch_query_unavailable`

## 签字

- 批准人签字：未签署
- 执行人签字：未签署
- 时间：2026-04-17 11:13:39 CST
