# BLOCK-002 Self Check

## Anti-Cheat
- 删除测试换取通过：否
- 降低断言强度换取通过：否
- 新增默认成功分支伪造完成：否
- 将异常降级为 warning：否
- 静默吞掉不确定状态：否
- 共享状态绕过持久化幂等：否

## Fail-Closed
- `claim_or_load_intent` 返回 `STORAGE_ERROR` 时，BUY 路径直接跳过（不创建新 intent，不发送订单）。
- `EXISTING_NON_TERMINAL` 且状态为 `SENDING`/`ACK_UNKNOWN`/`MANUAL_REQUIRED` 时，不允许直接重发。
- 发送前强制 `NEW -> SENDING` CAS，CAS 失败即阻断发送。

## Gate Snapshot
- static: pass
- tests: pass
- behavior: pass
- docs: pass
- block-item: pass

## Supplement Proof Snapshot
- Atomic claim/load: `test_claim_or_load_is_atomic_under_concurrent_claims` passed.
- NEW vs SENDING recovery split: `test_new_and_sending_recovery_paths_are_distinct` passed.
- MANUAL_REQUIRED lock: `test_manual_required_blocks_new_intent_and_resend` passed.
- Broker accepted but local ACK unknown: `test_broker_ack_unknown_probe_reuses_same_uuid_and_blocks_resend` passed.

## Known Residual Risk (Out of BLOCK-002 Scope)
- 未实现 BLOCK-003 single-writer lock 级别的全局串行化；虽然持久化 claim 可以阻止同一意图重复创建，但跨进程其他竞态仍需要后续阻断项收敛。
