# Runtime State Recovery Runbook (BLOCK-001)

## Scope
- This runbook covers runtime truth recovery for:
  - order intents (pending derived from non-terminal intents)
  - control state
  - risk state
  - reconciliation state
  - position snapshots
- `/tmp` artifacts and `runtime_state_path` / `control_path` are export/debug outputs only.

## Non-Truth Inputs (Explicitly Disabled)
- 启动恢复裁决不会读取以下路径作为真相输入：
  - `runtime_state_path`（例如 `runtime_state.json`）
  - `control_path`（例如 `control.json`）
  - `/tmp` 下任何历史快照/报告文件
- 这些文件仅用于：
  - 导出展示（dashboard/debug）
  - 人工排查（forensics）
  - 非裁决性可观测性输出

## Truth Sources and Priority
- Primary truth store: SQLite at `STATE_STORE_PATH`.
- Live broker facts (open orders, recent fills, wallet positions) override DB snapshot fields where applicable.
- Priority by domain:
  - `pending`: `order_intents`（单一真相）+ broker 证据修正 intent 状态
  - `positions`: broker quantity/notional > DB metadata
  - `control`: DB only
  - `risk`: recomputed from broker/account + reconciled runtime
  - `reconciliation`: recomputed at startup, DB is historical reference

## Pending 单一真相定义
- 真相定义：`pending = { intent in order_intents | intent.status ∉ terminal }`
- `pending_orders` 仅为运行期缓存结构，由 `order_intents` 派生，不参与启动恢复裁决。
- 旧快照里的 `pending_orders` 字段即使存在，也不会在恢复阶段作为第二真相源被读取。

## Startup Recovery Sequence (Atomic)
1. Acquire single-writer lock.
2. Set startup not-ready state (BUY disabled).
3. Open SQLite and apply migrations.
4. Load DB runtime truth bundle.
5. Pull broker facts (live mode).
6. Apply conflict matrix and write reconciled runtime to memory.
7. If unresolved conflicts exist, keep reduce-only and block BUY.
8. Persist reconciled snapshot back to SQLite.
9. Export debug files (`state.json`, `control.json`) as best-effort only.
10. Move to ready state only after recovery completes.

## Conflict Matrix
- `pending`（DB 有 intent，broker 无 open/fill 证据）  
  - 优先级：不自动判终态，保留 DB intent（`posted`）  
  - 自动裁决：可（标记 `AMBIGUOUS_PENDING`，写入冲突原因）  
  - 是否阻断 BUY：是  
  - 是否进入 reduce-only：是
- `pending`（broker open，DB 无 intent）  
  - 优先级：broker open 优先，生成 `broker_recovered_intent`  
  - 自动裁决：可（写 `recovered_source=broker` + `recovery_reason`）  
  - 是否阻断 BUY：是（直到一致性回写完成）  
  - 是否进入 reduce-only：是
- `positions`（DB 有仓位，broker 无仓位）  
  - 优先级：仅当有 recent fills/settlement/close 证据才允许自动清仓；无证据保留 DB 仓位  
  - 自动裁决：部分可（有证据可自动闭合；无证据标记 `AMBIGUOUS_POSITION`）  
  - 是否阻断 BUY：是（无证据场景）  
  - 是否进入 reduce-only：是（无证据场景）
- `control`（DB 不可读/结构非法/语义非法）  
  - 优先级：fail-close 控制位（manual + pause_opening + reduce_only）  
  - 自动裁决：可（直接写入 fail-close 控制态）  
  - 是否阻断 BUY：是  
  - 是否进入 reduce-only：是
- `risk`（账户快照与恢复状态冲突或陈旧）  
  - 优先级：broker/account 最新快照优先，DB risk 仅作恢复初值  
  - 自动裁决：可（重算 risk_state）；若不可验证则沿用保守态  
  - 是否阻断 BUY：是（快照陈旧/冲突超阈值）  
  - 是否进入 reduce-only：是
- `reconciliation`（DB 记录与启动校验不一致）  
  - 优先级：启动期实时 reconciliation 结果优先  
  - 自动裁决：可（重算状态并覆盖历史摘要）  
  - 是否阻断 BUY：是（`status != ok/pass`）  
  - 是否进入 reduce-only：是

## Validation Commands
- Recovery success and tmp deletion:
  - `python3 scripts/verify_runtime_persistence.py`
- Conflict path blocks BUY:
  - `python3 scripts/verify_restart_recovery.py`

## Operator Notes
- Do not manually edit `/tmp` files to recover runtime truth.
- If `recovery_conflict` is active, clear conflict evidence first, then restart.
- Any unresolved recovery conflict must be treated as trading stop for BUY path.
