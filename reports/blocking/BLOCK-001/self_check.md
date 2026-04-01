# BLOCK-001 自检报告（验收补证轮次）

## 本轮目标
- 仅补充 BLOCK-001 的验收证据：
  1) `/tmp` 不参与恢复裁决
  2) pending 单一真相
  3) 恢复失败硬阻断 BUY

## 本轮改动
- `tests/test_tmp_deletion_recovery.py`：新增脏 `/tmp` 导出不影响恢复真相用例。
- `tests/test_runtime_state_persistence.py`：新增旧 `pending_orders` 字段不参与恢复裁决用例。
- `tests/test_broker_db_conflict_blocks_buy.py`：增强冲突后 `startup_ready=false` + `REDUCE_ONLY` + BUY 不执行断言。
- `src/polymarket_bot/runner.py`：将 `recovery_conflict` 注入 startup checks，确保 `startup_ready` 显式变为 false。
- `scripts/verify_runtime_persistence.py`：覆盖“删除 /tmp 后恢复”+“脏 /tmp 不干扰裁决”。
- `scripts/verify_restart_recovery.py`：覆盖恢复冲突后 BUY 硬阻断。

## 反作弊自检
- 是否通过删除测试来换取通过：否
- 是否通过降低断言强度来换取通过：否
- 是否通过新增默认成功分支来伪造完成：否
- 是否把异常改成 warning：否
- 是否存在静默失败路径：否（冲突进入 `recovery_conflict` 并阻断 BUY）
- 是否存在共享状态暗门：否（恢复裁决只使用 DB + broker 证据）

## 结论
- 本轮补证门禁与验证脚本均通过；BLOCK-001 验收证据完整。
