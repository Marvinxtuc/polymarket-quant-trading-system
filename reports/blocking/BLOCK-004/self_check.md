# BLOCK-004 自检报告

- BLOCK-ID: `BLOCK-004`
- 目标: `startup/reconciliation/stale data` 全部 fail-closed，AdmissionGate 作为唯一准入真相。
- 状态: `DONE`
- 验收补证轮次: `2026-03-26`

## 交付完整性
- [x] 代码实现完成
- [x] 新增测试通过
- [x] 相关旧测试回归通过
- [x] 验证脚本通过
- [x] 文档更新完成（README + runbook）
- [x] `gate_block_item.sh BLOCK-004` 全门禁通过

## 反作弊自检
- 是否通过删除测试换取通过：**否**
- 是否通过降低断言强度换取通过：**否**
- 是否通过新增默认成功分支伪造完成：**否**
- 是否把异常降级成 warning：**否**
- 是否仍存在 fail-open 主路径：**否**（AdmissionGate `opening_allowed=false` 时 BUY 被硬拒绝）

## 关键验证点
- 启动失败会自动阻断 BUY，并保持保护态。
- 运行中 `reconciliation fail / stale snapshot / stale event stream / ledger diff` 均触发 fail-closed。
- `HALTED` 采用动作白名单；`REDUCE_ONLY` 禁止净敞口增加（含间接路径）。
- `/api/state` 暴露 admission 裁决、reason_codes、关键证据摘要。
- 即使 legacy `trading_mode` 兼容字段被伪装为 `NORMAL`，AdmissionGate 仍拒绝 BUY。

## 仍需关注
- 当前 single-writer 与幂等方案仍是其他 block 的独立边界；本轮未扩散修改。

## 本轮小修正
- `tests/test_runner_control.py` 的 `_DummyBroker` 补齐 `heartbeat/get_order_status/list_order_events`，以适配当前运行期退化路径覆盖。
- `tests/test_runner_control.py` 的 claim 状态断言改为大写枚举（`CLAIMED_NEW/EXISTING_NON_TERMINAL`），与当前持久化幂等返回值一致。
