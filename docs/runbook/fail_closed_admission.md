# BLOCK-004 Fail-Closed Admission Gate

## Scope
本运行手册定义 BLOCK-004 的交易准入门禁：当关键状态不可信时，系统自动 fail-closed，阻断 BUY，并进入 `REDUCE_ONLY` 或 `HALTED`。

## Single Truth
- 准入唯一真相：`AdmissionGate` 的裁决结果（`mode/opening_allowed/reduce_only/halted/reason_codes`）。
- 旧兼容字段（如 `trading_mode`）仅用于兼容展示，不得绕过 AdmissionGate。
- Admission 持久化快照仅保存“裁决输出 + reason_codes +证据摘要”，不是原始证据源。

## Runtime Semantics
- `NORMAL`
  - `opening_allowed=true`
  - 允许自动开仓/平仓
- `REDUCE_ONLY`
  - `opening_allowed=false`
  - 禁止任何增加净敞口动作（包括直接 BUY，及 replace/modify/retry 导致的间接增仓）
  - 允许：取消 pending BUY、风险收缩（SELL/减仓）、必要状态持久化
- `HALTED`
  - `opening_allowed=false`
  - 默认禁止所有自动订单提交
  - 仅允许白名单动作：
    - `sync_read`
    - `state_evaluation`
    - `cancel_pending_buy`
    - `persist_state_update`
  - 仅在 `operator_emergency_stop` 下额外允许 `operator_emergency_flatten_sell`

## Lifecycle / Latch
- 启动阶段先保护态（`opening_allowed=false`），再用新鲜证据重评估。
- 运行中退化（snapshot stale / event stream stale / reconciliation fail / ledger diff 超阈值 / recovery unresolved）必须自动进入保护态。
- 自动型锁存（auto latch）可在连续健康周期后自动解除。
- 人工型锁存（manual latch）必须人工解除，不可自动恢复，至少包括：
  - `operator_emergency_stop`
  - `operator_manual_reduce_only`
  - `persistence_fault`
  - `admission_gate_internal_error`
  - `recovery_conflict_unresolved_manual`

## [E. FAIL-CLOSED DECISION TABLE]
| 场景 | opening_allowed | reduce_only | halted | auto_recover | manual_confirmation_required |
|---|---:|---:|---:|---:|---:|
| startup checks fail | false | true | false | false | false |
| reconciliation fail | false | true | false | false | false |
| stale account snapshot | false | true | false | false | false |
| stale broker event stream | false | true | false | false | false |
| ledger diff 超阈值 | false | true | false | false | false |
| ambiguous pending unresolved | false | true | false | false | false |
| recovery conflict unresolved (auto) | false | true | false | false | false |
| recovery conflict unresolved (manual) | false | true | false | false | true |
| persistence fault | false | true | true | false | true |
| admission gate internal error | false | true | true | false | true |
| operator emergency stop | false | true | true | false | true |
| operator manual reduce-only | false | true | false | false | true |
| 自动恢复暖机期 | false | true | false | true | false |
| 全部证据可信 | true | false | false | false | false |

## Evidence Inputs
AdmissionGate 每周期评估以下输入：
- `startup_ready` / `startup_failure_count`
- `reconciliation.status`
- `account_snapshot_age_seconds`
- `broker_event_sync_age_seconds`
- `internal_vs_ledger_diff`
- `ambiguous_pending_orders`
- `recovery_conflict_count`
- `persistence_status`
- `operator_pause_opening` / `operator_reduce_only` / `operator_emergency_stop`

## Web/API Operator Visibility
`/api/state` 必须暴露：
- `admission.mode`
- `admission.opening_allowed`
- `admission.reduce_only`
- `admission.halted`
- `admission.reason_codes`
- `admission.evidence_summary`（至少包含）
  - `account_snapshot_age_seconds`
  - `broker_event_sync_age_seconds`
  - `ledger_diff`
  - `reconciliation_status`

## Verification
- 行为验证：
  - `PYTHONPATH=src .venv/bin/python scripts/verify_fail_closed_startup.py`
  - `PYTHONPATH=src .venv/bin/python scripts/verify_untrusted_state_blocks_buy.py`
- 全门禁：
  - `bash scripts/gates/gate_block_item.sh BLOCK-004`
