# BLOCK-005 Kill Switch Broker 终态确认

## 目标
把 `pause_opening / reduce_only / emergency_stop` 从本地布尔位升级为 broker 终态状态机。  
只有确认 broker 侧 BUY 风险已终态，系统才允许退出保护流程。

## 状态机
- `REQUESTED`：接收到控制位，尚未执行取消动作。
- `CANCELING_BUY`：正在对本地 pending BUY 与 broker open BUY 发起取消。
- `WAITING_BROKER_TERMINAL`：已发取消请求，等待 broker 终态确认。
- `SAFE_CONFIRMED`：broker 侧 BUY 风险已确认终态。
- `FAILED_MANUAL_REQUIRED`：取消/查询失败超限，进入人工介入锁存态。

## 关键语义
- `pause_opening`
  - 立即阻断新 BUY。
  - 不要求 broker 取消确认。
- `reduce_only`
  - 阻断新 BUY。
  - 必须清理 pending BUY，并等待 broker BUY 风险终态。
- `emergency_stop`
  - 最严格保护态：阻断新 BUY + `halted=true` + `latched=true`。
  - broker 未安全前不得解除。

## Broker terminal 判定（保守）
- 以下都视为**不安全**：
  - broker 仍存在 BUY open order；
  - 订单状态为 `cancel_requested/requested/queued/pending_cancel`；
  - 取消接口仅返回 `unknown/cancel_unknown/ambiguous`，但 broker 终态仍未确认；
  - 仅本地 pending 清空但 broker 无终态证据。
- 以下才视为**安全**：
  - broker BUY 订单全部终态（如 `canceled/filled/rejected/failed/expired`），且无 non-terminal BUY。

## Cancel Unknown Handling
- 若 broker cancel 返回 `requested`，系统记录为非终态取消请求，继续等待 broker 终态。
- 若 broker cancel 返回 `unknown`，系统记录 `broker_status=cancel_unknown`，语义是“请求是否生效不确定”。
- `cancel_unknown` 不得被解释为已取消、已安全或可恢复 BUY。
- 在 `cancel_unknown` 场景下，值守动作应是继续查询 broker 终态；若多轮查询后仍无锚点，按人工介入处理。

## 重启恢复
- kill switch inflight 状态持久化到 SQLite runtime truth（`runtime_state.kill_switch`）。
- 重启后先恢复该状态，再继续 `WAITING_BROKER_TERMINAL` 探测流程。
- 若证据不足，不得自动放开 BUY。

## [E. KILL SWITCH DECISION TABLE]
| 场景 | opening_allowed | reduce_only | halted/latched | broker_safe_confirmed | manual_required | auto_recover |
|---|---:|---:|---:|---:|---:|---:|
| pause_opening 触发 | false | false | false/true | true | false | true |
| reduce_only 触发 | false | true | false/true | false | false | true |
| emergency_stop 触发 | false | true | true/true | false | false | false |
| cancel requested / queued | false | true | (mode相关)/true | false | false | true |
| broker 仍有 open BUY | false | true | (mode相关)/true | false | false | true |
| broker terminal confirmed | false（若控制位仍开）/true（控制位清除后） | mode相关 | mode相关/可解除 | true | false | true |
| cancel/query timeout | false | true | true/true | false | true | false |
| restart during inflight cancel | false | true | (mode相关)/true | false | false | true |

## Web/API 对外字段
`/api/state.kill_switch` 至少暴露：
- `mode_requested`
- `phase`
- `opening_allowed`
- `reduce_only`
- `halted`
- `latched`
- `broker_safe_confirmed`
- `manual_required`
- `reason_codes`
- `open_buy_order_ids`
- `non_terminal_buy_order_ids`
- `cancel_requested_order_ids`

## 验证命令
- `PYTHONPATH=src .venv/bin/python scripts/verify_kill_switch_terminal.py`
- `PYTHONPATH=src .venv/bin/python scripts/verify_reduce_only_terminal_cleanup.py`
- `bash scripts/gates/gate_block_item.sh BLOCK-005`
