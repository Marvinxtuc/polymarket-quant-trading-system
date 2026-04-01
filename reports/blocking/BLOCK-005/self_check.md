# BLOCK-005 Self Check

## Scope Confirmation
- 仅处理 kill switch broker 终态状态机（pause_opening / reduce_only / emergency_stop）。
- 未改动控制面鉴权、告警系统、预算与全局风控逻辑。
- 未并入 BLOCK-006/007/008。

## Completion Checklist
- [x] 代码改动完成（状态机、持久化、重启恢复、API暴露）。
- [x] 新增测试全部通过。
- [x] 相关旧测试回归通过（runner/web/paper 相关套件）。
- [x] 验证脚本通过。
- [x] 文档已更新（README + runbook）。
- [x] gate_block_item.sh BLOCK-005 通过。

## Anti-Cheat Self Audit
- 是否通过删除测试来换取通过：否
- 是否通过降低断言强度来换取通过：否
- 是否通过新增默认成功分支来伪造完成：否
- 是否把异常改成 warning：否
- 是否仍存在静默失败路径：否（关键失败进入 `FAILED_MANUAL_REQUIRED` 并阻断 BUY）
- 是否仍存在 broker 未安全即放开 BUY：否

## Known Risks
- 当 broker 查询长期不可用时会进入 `FAILED_MANUAL_REQUIRED` 并保持阻断，这会牺牲可用性但符合 fail-closed 目标。
- 本阻断项不包含多机分布式一致性扩展（不在 BLOCK-005 范围内）。

## Acceptance Evidence Refresh
- 2026-03-26 已复跑 proof 对应测试与全部 gates，结果全部通过。
