# Polymarket Bot 实盘上线清单

更新日期：2026-03-20

## 目标

把系统从“可演示/可本地验证”推进到“可安全实盘”。

## A. 先决条件（P0）

- [ ] 基础环境完整
  - [ ] 虚拟环境已建并可复现：`python3 -m venv .venv && . .venv/bin/activate`
  - [ ] 依赖安装：`pip install -e .` 与 live 依赖 `pip install -e '.[live]'`
  - [ ] `.env` 以 `.env.example` 为模板完整配置（至少配置：`DRY_RUN`,`POLL_INTERVAL_SECONDS`,`BANKROLL_USD`,`RISK_PER_TRADE_PCT`,`DAILY_MAX_LOSS_PCT`,`MAX_OPEN_POSITIONS`,`PRIVATE_KEY`,`FUNDER_ADDRESS`）
- [ ] 安全检查
  - [ ] `make env-check` 必须成功或明确确认 `--warn-only` 输出原因
  - [ ] 禁止将 `.env` 纳入 Git（确认 `.gitignore` 生效）

## B. 一键验证闭环（P0）

- [ ] 一键验证命令可连续通过
  - [ ] `make one-click`
  - [ ] `make verify`
  - [ ] `make full-validate`
  - [ ] `make fault-drill`
  - [ ] `make release-gate`
  - [ ] `make readiness-brief`
  - [ ] `make monitor-scheduler-smoke`
  - [ ] `make alert-smoke`
  - [ ] `make live-smoke-preflight`
- [ ] 验证项
  - [ ] `make test` 通过
  - [ ] `start_poly_stack.sh` 启动后约 15 秒内可健康访问 8787；首次 `state.json` 可由 daemon bootstrap state 提前落盘，不必等待首轮重 cycle 完成
  - [ ] `scripts/verify_stack.sh` 返回 `OK` 且 `state.ts` 新鲜（`age <= 3 * poll_interval`）
  - [ ] `make full-validate` 返回 `validation_status: PASS` 且 `flow_standard_met: True`
  - [ ] `make fault-drill` 返回 `PASS`
  - [ ] `make release-gate` 返回 `READY`；若为 `BLOCKED` 或 `CAUTION`，必须逐项清空 blocker/advisory 后再放行
  - [ ] `make readiness-brief` 可快速核对当前 checkpoint、剩余时间和 blocker 是否与 `release-gate` 一致
  - [ ] `make monitor-scheduler-smoke` 返回 `PASS`，并确认 nohup pid 死亡时 `monitor_scheduler_status.sh` 会明确显示 `status=stale`
  - [ ] `make alert-smoke` 在配置远程凭据后返回可发送或实投通过；未配置远程凭据时必须视为 blocker
    - [ ] webhook 方案至少配置 `NOTIFY_WEBHOOK_URL` 或 `NOTIFY_WEBHOOK_URLS`
    - [ ] Telegram 方案至少配置 `NOTIFY_TELEGRAM_BOT_TOKEN` 和 `NOTIFY_TELEGRAM_CHAT_ID`
  - [ ] `make live-smoke-preflight` 必须通过后，才允许执行真实 `live_clob_type2_smoke.py`
  - [ ] 真实 `make live-smoke` 执行后，确认已生成 `live_smoke_execution_summary.json`
  - [ ] 真实 smoke 统一通过 `LIVE_SMOKE_ACK=YES LIVE_SMOKE_TOKEN_ID=<token_id> make live-smoke` 执行，不再手拼脚本命令
  - [ ] 若 `paper` rehearsal 正占用 `8787`，允许用 `STACK_WEB_PORT=8788` 临时拉起 fresh live stack 完成 preflight，不必先停 rehearsal
  - [ ] 明确区分“链路打通”和“运营准入”：
    - [ ] `validation_status: PASS` 只代表状态、monitor、reconciliation、replay 链路已打通
    - [ ] `operational_readiness=OBSERVE` 代表仍处在观察窗口，不能当成“可放量/可开仓”结论
- [ ] 失败处理
  - [ ] 先执行 `./scripts/stop_poly_stack.sh`（如需调试，再按进程名定向 `pkill`）
  - [ ] 清空当前实例日志：
    - [ ] `WEB_LOG="$(PYTHONPATH=src .venv/bin/python scripts/runtime_paths.py web_log_path)"; : > "$WEB_LOG"`
    - [ ] `BOT_LOG="$(PYTHONPATH=src .venv/bin/python scripts/runtime_paths.py bot_log_path)"; : > "$BOT_LOG"`

## C. 风控与交易安全（P0）

- [ ] 资金与仓位保护
  - [ ] `BANKROLL_USD > 0`
  - [ ] `RISK_PER_TRADE_PCT * BANKROLL_USD >= 10`（单笔至少 10 USD 才能避免极小单）
  - [ ] `DAILY_MAX_LOSS_PCT` 上限满足策略预期（如 <= 0.05）
  - [ ] `MAX_OPEN_POSITIONS >= 1`
- [ ] 关键控制位可用
  - [ ] 控制 API 生效：`pause_opening/reduce_only/emergency_stop`
  - [ ] `emergency_stop` 与 `reduce_only` 在 UI/接口可快速触发并可回退
  - [ ] 灰度前验证阶段保持 `decision_mode=manual` 且 `pause_opening=true`
  - [ ] 此时 UI `/api/state` 中应看到：
    - [ ] `control.pause_opening=true`
    - [ ] `trading_mode.mode=REDUCE_ONLY`
    - [ ] `trading_mode.reason_codes` 包含 `operator_pause_opening`

## D. 运行一致性（P1）

- [ ] 状态面板口径一致
  - [ ] “执行模式”显示与 `DRY_RUN` 一致
  - [ ] 预算显示为 `tracked_notional_usd / available_notional_usd`
  - [ ] “可用预算”“槽位利用率”不再出现未定义字段
  - [ ] 关键控制面字段一致：
    - [ ] `trading_mode`
    - [ ] `control`
    - [ ] `reconciliation`
    - [ ] `persistence`
- [ ] 数据完整性
  - [ ] `positions/wallets/sources/orders/alerts` 字段存在且可为空但不缺字段
  - [ ] 控制状态变更（`updated_ts`）可追踪

## E. 运行可靠性（P1）

- [ ] 进程自愈
  - [ ] 启动脚本在无 launchctl 权限的机器可降级到直跑：`scripts/start_poly_stack.sh`
  - [ ] 8787 端口冲突时有清理与回退日志
  - [ ] daemon 重启后会先写 bootstrap state，`verify_stack` 不再依赖首轮重 cycle 才能拿到新鲜 `state.ts`
  - [ ] `./scripts/monitor_scheduler_status.sh` 若显示 `status=stale`，必须视为调度器未运行，需检查 `log=` 指向的日志并重新安装 scheduler
- [ ] 资源与清理
  - [ ] 启动后 2 分钟内无频繁重启
  - [ ] 无残留僵尸监听（`lsof -nP -iTCP:8787 -sTCP:LISTEN`）

## F. 实盘前演练（P1）

- [ ] 低规模演练（纸面/模拟）
  - [ ] 先 `DRY_RUN=true` 跑 24 小时无异常
  - [ ] 启动命令：`make rehearse-24h-dry-run`
  - [ ] 进度查看：`make rehearse-24h-progress`
  - [ ] 完成后执行：`make rehearsal-finalize`
  - [ ] 模拟 `emergency_stop` 演练一次并恢复
- [ ] 切换 live 的红线控制
  - [ ] 切 `DRY_RUN=false` 前做 5 分钟干跑验证
  - [ ] 干跑验证阶段至少完成一次：
    - [ ] `make full-validate`
    - [ ] `make fault-drill`
    - [ ] `make rehearsal-finalize`
    - [ ] 核对 dashboard 的 `trading_mode / reconciliation / persistence`
    - [ ] 在 `pause_opening=true` 下观察至少 1 个完整 30m monitor 窗口
  - [ ] 首单前确认真实地址与 funder 正确

## G. 监控告警（P2）

- [ ] 监控覆盖
  - [ ] API 健康（`/api/state`）
  - [ ] 错误计数（rejected/skip）
  - [ ] 日内损失率与槽位利用率
- [ ] 告警策略
  - [ ] 关键失败触发人通知（至少 webhook 或 Telegram）
  - [ ] `startup gate / account stale / reconcile protect / persistence fault / HALTED` 至少有一条远程通知链路

## H. 回退手册（SOP）

### 快速回退（5 分钟）
1. UI 停止开仓：进入控制面板点“暂停开仓”
2. 启动紧急停止：点“紧急退出”
3. 切回安全模式：设置 `DRY_RUN=true` 并重启服务
4. 停止服务
   - [ ] 统一执行：`./scripts/stop_poly_stack.sh`
   - [ ] 如需停监控调度：`./scripts/stop_monitor_reports.sh`
5. 持仓核对：`STATE_PATH="$(PYTHONPATH=src .venv/bin/python scripts/runtime_paths.py state_path)"`，比对钱包当前仓位与 `$STATE_PATH`

### 回退确认
- [ ] `make verify` 通过（环境侧）
- [ ] Web API 恢复到最近 1 次有效心跳
- [ ] `STATE_PATH="$(PYTHONPATH=src .venv/bin/python scripts/runtime_paths.py state_path)"` 可读取且 `control.emergency_stop`/`reduce_only` 正确落盘

## I. 上线发布标准

- [ ] 本清单 P0 全部通过，P1 全数通过，P2 至少 80%
- [ ] 记录“上线时刻、版本、配置 hash、上游数据源状态、钱包地址”三元组
- [ ] 决策人签字后进入 24 小时观察期
- [ ] 会签记录已落到 [production_signoff_template.md](/Users/marvin.xa/Desktop/Polymarket/production_signoff_template.md)
- [ ] 发布记录已落到 [production_release_record_template.md](/Users/marvin.xa/Desktop/Polymarket/production_release_record_template.md)
- [ ] 值守交接模板已准备并指定接班人：[operations_handoff_template.md](/Users/marvin.xa/Desktop/Polymarket/operations_handoff_template.md)
