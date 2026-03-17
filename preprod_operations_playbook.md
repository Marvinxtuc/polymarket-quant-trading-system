# Polymarket Bot 上线运行手册（实盘前后）

本手册用于“paper→live”切换后的实时监控与回退动作，配套已有的验收清单使用。

## 一、前 4 小时监控模板（T0 ~ T4h）

| 时间点 | 观察项 | 通过标准 | 异常处理 |
|---|---|---|---|
| T0（切换后 0~5 分钟） | `make verify`、`/api/state` | `make verify` 成功；`state.ts` 新鲜（`age <= 3 * poll`） | 立即执行回退流程（下文），先排查端口与服务状态 |
| T0+5 分钟 | 控制面板状态 | `pause_opening=false`、`reduce_only=false`、`emergency_stop=false`；按钮状态与 API 一致 | 调用控制接口刷新状态，必要时重启 |
| T0+10 分钟 | 风险预算与执行行为 | `tracked_notional_usd` 不出现异常突增；有成交时无明显异常日志 | 记录异常信号与市场，先 `pause_opening=true` |
| T0+15 分钟 | 仓位边界 | `open_positions <= max_open_positions` | 触发上限保护：`pause_opening=true` 并复盘 |
| 每 30 分钟（T0+30、+60、+90） | 日内风控 | `daily_loss_used_pct` 持续可控；`slot_utilization_pct` 不突涨 | 若偏离，`pause_opening=true`，必要时 `reduce_only=true` |
| 每 30 分钟 | 日志健康 | 无连续失败风暴（reject/time_exit_fail） | 连续异常可先 `pause_opening=true` 再 `reduce_only=true` |
| T0+120 分钟 | 策略行为一致性 | 信号、成交、拒单比率可解释；无重复下单异常 | 人工抽样检查控制与钱包状态 |
| T0+180 分钟 | 轻量对账 | 链上（如可查）与 `positions` 方向一致 | 不一致时暂停开仓，核对持仓来源 |
| T0+240 分钟 | 里程碑复盘 | 无重大告警，4 小时无回退 | 允许进入 12 小时计划 |

## 二、前 12 小时监控模板（T0 ~ T12h）

| 时间点 | 观察项 | 通过标准 | 异常处理 |
|---|---|---|---|
| 每 1 小时 | `make verify`（每小时 1 次） | 连续通过；`state` 可读、时间新鲜 | 首次失败先执行回退流程，并保留日志 |
| 每 1 小时 | 风险健康 | `daily_loss_used_pct` 低于设定上限；`notional_utilization_pct` 平稳 | 接近上限时 `pause_opening=true` |
| 每 1 小时 | 订单质量 | 成交/失败可解释，失败原因可追踪 | 连续失败抬升则 `reduce_only=true` |
| 每 2 小时 | 面板可用性 | `execution_mode / broker_name / control` 持续与预期一致 | web 不响应时重启服务并检查端口 |
| 每 2 小时 | 运行可靠性 | 无端口漂移、无反复重启 | 处理旧进程/端口占用，验证 PID 与监听归属 |
| 每 2 小时 | 仓位行为 | `open_positions` 与策略预期一致 | 长期贴边运行时暂停开仓观察 |
| 第 6 小时 | 中点复盘 | 形成简报：信号量、成交率、拒单率、损失占比 | 偏离预期时降风险参数观察一轮 |
| 第 12 小时 | 稳态判定 | 无重大告警、日志可追溯 | 形成“是否放量”的业务会签决定 |

## 三、回退阈值化规则

> 执行顺序：先记录时间戳和关键快照，再执行以下动作。

| 指标 | 触发阈值 | 第一层 | 第二层 |
|---|---:|---|---|
| `state age` | `> 2 * poll_interval` 持续 2 次采样 | `pause_opening=true` | 再持续失败则 `reduce_only=true`，必要时 DRY RUN 重启 |
| `open_positions` | 长期接近上限（`> max_open_positions - 1`） | `pause_opening=true` | 持续 2~3 周期则 `reduce_only=true` |
| `daily_loss_used_pct` | `>= 50%` | `pause_opening=true` | `>= 75%` 则 `reduce_only=true` 并复盘 |
| 近 30 分钟 `reject` 占比 | `> 25%` 且连续 | `pause_opening=true` | 持续抬升改 `reduce_only=true` |
| 连续失败订单 | 5 条连续失败 | `pause_opening=true` | `reduce_only=true` + 暂停 wallet discovery |
| `tracked_notional_usd` 单周期异常突增 | > 前周期 2 倍 | `pause_opening=true` 并定位来源 | 确认误信号后降权钱包 / 恢复 |
| `emergency_stop` 触发 | 任意时刻 | 仅执行减仓/清仓流 | 持续异常则 DRY RUN + 复核控制 |
| 端口/进程冲突 | 8787 被非本服务占用 | 停止旧进程、清理端口 | 连续失败则回退 DRY RUN 停止新开仓 |

## 四、快手命令（值守时直接执行）

```bash
make verify
curl -s http://127.0.0.1:8787/api/state | jq '.summary, .control, .config'
curl -s http://127.0.0.1:8787/api/control | jq .
lsof -nP -iTCP:8787 -sTCP:LISTEN
tail -n 120 /tmp/poly_runtime_data/poly_bot.log
tail -n 120 /tmp/poly_runtime_data/poly_web.log
```

## 五、操作动作（5 分钟标准流程）

1. `pause_opening=true`
2. 仍异常：`reduce_only=true`
3. 仍异常或资金风控失控：改 `DRY_RUN=true`，`make one-click`
4. 快速对账：`state.json`、`open_positions`、`orders`、`positions`
5. 恢复前必须执行 `make verify` 两次成功

## 六、12 小时复盘模板（可直接粘贴）

### 12h Live Checkpoint Report（模板）

| 时间 | 区间 | 关键指标 | 观测结果 | 告警 | 动作 | 结果/结论 |
|---|---|---|---|---:|---|---|
| ___ | T0~T1 | state 新鲜度/服务健康 | `age=__s`, `make verify`=通过/失败 | `pass/fail` | 无 | 结论 |
| ___ | T1~T2 | 风控状态 | `open_positions=__/__`, `tracked_notional_usd=$__`, `available_notional_usd=$__` | `pass/fail` | 无 | 结论 |
| ___ | T1~T2 | 订单质量 | `filled=__`, `reject=__`, `reject_reason=__` | `pass/fail` | 无 | 结论 |
| ___ | T2~T4 | 控制行为 | `pause_opening=__`, `reduce_only=__`, `emergency_stop=__` | `pass/fail` | 无 | 结论 |
| ___ | T2~T4 | 仓位与策略一致性 | `sources/wallets` 变动说明 | `pass/fail` | 无 | 结论 |
| ___ | T4~T6 | 失败率与风控偏差 | `daily_loss_used_pct=__%`, `slot_utilization=__%` | `pass/fail` | 必要时 pause/reduce | 结论 |
| ___ | T6~T8 | 运行稳定性 | 进程/端口异常次数 | `pass/fail` | 清理/重启 | 结论 |
| ___ | T8~T10 | 资金与风险上限 | `bankroll/utilization` 偏离说明 | `pass/fail` | 调参/观察 | 结论 |
| ___ | T10~T12 | 总体风险回归 | 异常累计统计（reject/skip/close） | `pass/fail` | 回退/维持 | 结论 |
| ___ | 复盘总结 | 主要事件、根因、下一步 | - | `pass/fail` | 后续动作 | 结论 |

### 12h 复盘记录示例填充项

- 交易周期：`YYYY-MM-DD HH:mm ~ HH:mm`
- 关键事件：
  - `__` 次控制切换（pause/reduce/emergency）
  - `__` 次状态重刷失败
  - `__` 次 reject
- 风险结论：`可放量 / 保持保守 / 立即回退`
- 是否进入观察窗口：`是 / 否`
- 下轮动作建议：
  - 风险参数：`risk_per_trade_pct=__`, `max_open_positions=__`, `token_add_cooldown_seconds=__`
  - 监控重点：`__`

## 七、12h 极简复盘（快速版）

时间段：T0~T12h

| 时间 | 指标摘要 | 告警 | 动作 | 结论 |
|---|---|---|---|---|
| T0~T1 | `state age / verify` | 无 / 有 | 无 / 回退动作 | 通过 / 未通过 |
| T1~T4 | `open_positions / tracked_notional_usd` | 无 / 有 | 无 / `pause_opening` | 通过 / 未通过 |
| T4~T8 | `daily_loss_used_pct / reject rate` | 无 / 有 | 无 / `reduce_only` | 通过 / 未通过 |
| T8~T12 | `port + service health` | 无 / 有 | 无 / 重启+回退检查 | 通过 / 未通过 |

### 一句总结
- 是否发生控制切换：是 / 否（`pause/reduce/emergency`）
- 异常次数：`state失联 __` / `reject __` / `重试 __` / `端口占用 __`
- 今日决策：`继续观察` / `缩小参数` / `立即回退 DRY_RUN`
- 下一步：`_____`
