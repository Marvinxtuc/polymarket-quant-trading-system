# BLOCK-003 Self Check

## Anti-Cheat
- 删除测试换取通过：否
- 降低断言强度换取通过：否
- 新增默认成功分支伪造完成：否
- 将异常降级为 warning：否
- 将失败分支改为默认成功：否
- 保留未持锁实例写侧门：否

## Fail-Closed Coverage
- 锁冲突：固定 `reason_code=single_writer_conflict`，入口进程退出码 `42`。
- live 模式 `ENABLE_SINGLE_WRITER=false`：入口拒绝启动（fail-close）。
- 锁目录异常（不可创建/不可写/非本地/无法确认）：拒绝启动 active writer。
- 未持锁 Web 实例：写接口返回 `503`；GET 路径禁用导出写入和缓存刷新副作用。
- StateStore 所有持久化写方法统一接入 writer guard，写前执行 ownership 二次校验。

## StateStore Mutation Methods Guarded
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

## Gate Snapshot
- static: pass
- tests: pass
- behavior: pass
- docs: pass
- block-item: pass

## Supplement Proof Snapshot
- LOCK before side effects: `test_main_acquires_lock_before_build_trader`, `test_daemon_acquires_lock_before_build_trader`, `test_web_main_acquires_lock_before_write_handler_registration` passed.
- Readonly web no side effects: `test_standby_get_state_has_no_export_side_effect`, `test_standby_get_blockbeats_does_not_fetch_or_cache_write` passed.
- StateStore full guard + deny on lost ownership: `test_all_mutation_methods_require_writer_guard`, `test_mutation_methods_fail_when_writer_not_active` passed.
- Live bypass deny: `test_live_mode_disallows_single_writer_bypass_in_main`, `test_live_mode_disallows_single_writer_bypass_in_daemon` passed.
- Crash handover invalidates old writer: `test_old_writer_loses_write_privilege_after_handover` passed.

## Acceptance Rerun (2026-03-26)
- Targeted proof suites rerun and all passed.
- Gate rerun status:
  - static: pass
  - tests: pass
  - behavior: pass
  - docs: pass
  - block-item: pass

## Known Residual Risk
- 当前锁实现是单机 `flock` 语义，不覆盖跨主机分布式双活；分布式 HA 需后续阻断项处理。
- Web 若需在独立进程保留可写控制面，必须确保该进程可获得同一 writer 锁，否则按设计降级只读。
