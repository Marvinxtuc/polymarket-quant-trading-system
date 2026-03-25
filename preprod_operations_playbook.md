# Polymarket Bot 上线运行手册（实盘前后）

本手册用于“paper→live”切换后的实时监控与回退动作，配套已有的验收清单使用。

## 一、前 4 小时监控模板（T0 ~ T4h）

| 时间点 | 观察项 | 通过标准 | 异常处理 |
|---|---|---|---|
| T0（切换后 0~5 分钟） | `make verify`、`/api/state` | `make verify` 成功；`state.ts` 新鲜（`age <= 3 * poll`）；bootstrap state 可在约 15 秒内出现 | 立即执行回退流程（下文），先排查端口与服务状态 |
| T0+5 分钟 | 控制面板状态 | 灰度验证阶段默认 `pause_opening=true`、`decision_mode=manual`；`trading_mode=REDUCE_ONLY`；按钮状态与 API 一致 | 调用控制接口刷新状态，必要时重启 |
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
| 每 1 小时 | 保护链演练 | `make fault-drill` 通过，至少确认 startup gate / persistence halt / reconcile ambiguity 三类保护仍可用 | 任一 drill 失败则停止放量，先修保护链 |
| 每 1 小时 | 验收与运营门禁 | `make full-validate` 可通过；注意区分 `validation_status=PASS` 与 `operational_readiness=OBSERVE/ESCALATE/BLOCK` | `PASS` 但 `OBSERVE` 时继续观察，不放量、不解除 `pause_opening` |
| 放行前最后一步 | 统一准入 gate | `make release-gate`，要求 `status=READY` | `BLOCKED/CAUTION` 时逐项清空 blocker/advisory，禁止放行 |
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
make full-validate
make fault-drill
make release-gate
make readiness-brief
make rehearsal-finalize
make rehearse-24h-dry-run
make rehearse-24h-progress
curl -s "http://127.0.0.1:8787/api/state?token=$POLY_CONTROL_TOKEN" | jq '.summary, .control, .trading_mode, .reconciliation, .persistence'
curl -s "http://127.0.0.1:8787/api/control?token=$POLY_CONTROL_TOKEN" | jq .
lsof -nP -iTCP:8787 -sTCP:LISTEN
BOT_LOG="$(PYTHONPATH=src .venv/bin/python scripts/runtime_paths.py bot_log_path)"; tail -n 120 "$BOT_LOG"
WEB_LOG="$(PYTHONPATH=src .venv/bin/python scripts/runtime_paths.py web_log_path)"; tail -n 120 "$WEB_LOG"
```

值守时先看 3 个结论再决定是否继续放量：

1. `validation_status / flow_standard_met`
2. `operational_readiness`
3. dashboard 里的 `trading_mode / reconciliation / persistence`

`make rehearse-24h-dry-run` 会把本机 stack 切到 `DRY_RUN=true` 的 paper 模式并在同一端口启动 24h 观察。
`make rehearsal-finalize` 会在 rehearsal 完成后统一执行最后一轮收尾检查；如果 rehearsal 还没跑满，它会安全返回 `PENDING`，不去乱跑后置动作。
当前默认启动路径是 direct + nohup；monitor scheduler 在 launchd 无法稳定访问仓库脚本时会自动回退到 namespaced nohup。
如果 `./scripts/monitor_scheduler_status.sh` 返回 `status=stale`，表示 method 文件还在，但记录的 nohup pid 已经死亡；此时不要把它当成“仍在后台运行”，应先看 `log=` 指向的日志，再重新执行 `make monitor-scheduler-install`。
值守前可以直接跑 `make monitor-scheduler-smoke`，它会演练一次“活 pid -> stale pid -> 重装恢复”的完整检查。
值守前还应跑两条前置检查：

1. `make alert-smoke`
   当前如果返回 `remote alert channel not configured`，必须先补 webhook / Telegram 再进入 live smoke。
   最低配置要求是：
   - webhook：`NOTIFY_WEBHOOK_URL` 或 `NOTIFY_WEBHOOK_URLS`
   - Telegram：`NOTIFY_TELEGRAM_BOT_TOKEN` + `NOTIFY_TELEGRAM_CHAT_ID`
2. `make live-smoke-preflight`
   只有它通过后，才允许进入真实 `live connectivity` 烟测。
3. 真实 smoke 统一入口：
   `LIVE_SMOKE_ACK=YES LIVE_SMOKE_TOKEN_ID=<token_id> make live-smoke`
   这个入口会先跑 preflight，并把单腿 notional 默认限制在小额范围；成功后还会写入 `live_smoke_execution_summary.json`。
4. 如果 `paper` rehearsal 正占着 `8787`，可以用
   `STACK_WEB_PORT=8788 START_STACK_DISABLE_LAUNCHCTL=1 ./scripts/start_poly_stack.sh`
   临时拉起 fresh 的 live stack，再做 live preflight；不需要先停掉 rehearsal。
5. 真正准备放行前，再跑一次：
   `make release-gate`
   这个入口会统一读取 `full_flow_validation_report.json`、`24h_dry_run_rehearsal.txt`、`alert_delivery_smoke.json`、`live_smoke_preflight.json`、`live_smoke_execution_summary.json` 和会签草案，给出 `READY / CAUTION / BLOCKED`。
6. 值守或等待 rehearsal 结束时，可以随时跑：
   `make readiness-brief`
   它会输出当前 release gate、checkpoint 数、最近一个 checkpoint 和 rehearsal 剩余时间，适合快速同步现场状态。

关键失败现在会通过 notifier 统一走一套报警口径，值守时重点关注：

1. `startup gate blocked`
2. `account state protect`
3. `reconciliation protect`
4. `HALTED · persistence fault` / `HALTED · trading stopped`

## 五、操作动作（5 分钟标准流程）

1. `pause_opening=true`
2. 仍异常：`reduce_only=true`
3. 仍异常或资金风控失控：改 `DRY_RUN=true`，`make one-click`
4. 快速对账：`state.json`、`open_positions`、`orders`、`positions`
5. 恢复前必须执行 `make verify` 两次成功

值守交接与上线留痕统一使用以下模板：

- [production_signoff_template.md](/Users/marvin.xa/Desktop/Polymarket/production_signoff_template.md)
- [production_release_record_template.md](/Users/marvin.xa/Desktop/Polymarket/production_release_record_template.md)
- [operations_handoff_template.md](/Users/marvin.xa/Desktop/Polymarket/operations_handoff_template.md)

## 六、12 小时复盘模板（可直接粘贴）

### 12h Live Checkpoint Report（模板）

| 时间 | 区间 | 关键指标 | 观测结果 | 告警 | 动作 | 结果/结论 |
|---|---|---|---|---:|---|---|
| ___ | T0~T1 | state 新鲜度/服务健康 | `age=__s`, `make verify`=通过/失败, `full-validate`=PASS/FAIL | `pass/fail` | 无 | 结论 |
| ___ | T1~T2 | 风控状态 | `open_positions=__/__`, `tracked_notional_usd=$__`, `available_notional_usd=$__` | `pass/fail` | 无 | 结论 |
| ___ | T1~T2 | 订单质量 | `filled=__`, `reject=__`, `reject_reason=__` | `pass/fail` | 无 | 结论 |
| ___ | T2~T4 | 控制行为 | `pause_opening=__`, `reduce_only=__`, `emergency_stop=__`, `trading_mode=__` | `pass/fail` | 无 | 结论 |
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
