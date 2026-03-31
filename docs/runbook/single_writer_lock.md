# Single Writer Lock Runbook (BLOCK-003)

## Scope
- 只覆盖 single-writer 锁、写入口 ownership 校验、双执行器冲突阻断。
- 不覆盖幂等主逻辑（BLOCK-002）、告警、鉴权、全局预算风控。

## Writer Scope 规则
- 统一规则：`<mode>:<identity>`
  - `mode`：`live` 或 `paper`
  - `identity`：
    - live：`FUNDER_ADDRESS`（小写）
    - paper：`WATCH_WALLETS` 首个钱包；缺失时 `default`
- 该规则会出现在：
  - 启动日志：`SINGLE_WRITER_SCOPE scope=...`
  - 验证脚本输出：`writer_scope=...`
  - 本 runbook 文档

## 锁类型与生命周期
- 锁类型：本地文件系统 `flock`（`WALLET_LOCK_PATH`）。
- 获取时机：在 `main.py` / `daemon.py` 进入 `build_trader()` 前获取。
- Web 控制面同样在 `web.py` 的 `build_handler()` 前尝试获取 writer 锁（仅在启用写 API 时）。
- 续租：`flock` 持有期即租期，写前通过 `assert_active()` 二次确认 ownership。
- 释放：进程退出或显式 `release()`。
- 冲突退出：
  - reason_code：`single_writer_conflict`
  - 退出码：`42`

## Fail-Close 规则
- live 模式下 `ENABLE_SINGLE_WRITER=false`：拒绝启动（fail-close）。
- 锁目录创建失败、不可写、权限异常：拒绝启动 active writer。
- 锁路径若无法确认本地文件系统：拒绝启动 active writer。
- 未持锁实例：
  - 不允许进入可写交易主循环（第二执行器在启动期即冲突退出）。
  - Web 写接口返回 `503`，`reason_code=single_writer_conflict`。
  - GET/只读接口禁副作用：不做导出写入、不做懒更新、不做恢复探测落盘。

## 写入口授权边界
- 允许写（必须通过 `assert_writer_active()`）：
  - `order_intents` 写入与状态推进
  - `control_state` 写入
  - `risk_state` 写入
  - `reconciliation_state` 写入
  - 恢复裁决写回（runtime truth）
- Web 写入口（`/api/control`、`/api/operator`、`/api/mode`、`/api/candidate/action`、`/api/journal/note`、`/api/wallet-profiles/update`）必须在 active writer 才可执行。

## StateStore 持久化写方法（已 guard）
- `save_runtime_state`
- `save_risk_state`
- `save_reconciliation_state`
- `replace_positions`
- `replace_order_intents`
- `update_intent_status`
- `save_runtime_truth`
- `claim_or_load_intent`
- `register_idempotency`
- `cleanup_idempotency`
- `save_control_state`

## 验证
- 行为验证：
  - `python3 scripts/verify_single_writer.py`
  - `python3 scripts/verify_lock_recovery.py`
- gate 总入口：
  - `./scripts/gates/gate_block_item.sh BLOCK-003`
