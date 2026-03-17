# Polymarket Bot 实盘上线清单

更新日期：2026-03-16

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
- [ ] 验证项
  - [ ] `make test` 通过
  - [ ] `start_poly_stack.sh` 启动后 8787 可健康访问 `/api/state`
  - [ ] `scripts/verify_stack.sh` 返回 `OK` 且 `state.ts` 新鲜（`age <= 3 * poll_interval`）
- [ ] 失败处理
  - [ ] 先执行 `pkill -f polymarket_bot.web` / `pkill -f polymarket_bot.daemon`（或重启 launchctl 服务）
  - [ ] 清空旧日志：`: > /tmp/poly_runtime_data/poly_web.log`、`: > /tmp/poly_runtime_data/poly_bot.log`

## C. 风控与交易安全（P0）

- [ ] 资金与仓位保护
  - [ ] `BANKROLL_USD > 0`
  - [ ] `RISK_PER_TRADE_PCT * BANKROLL_USD >= 10`（单笔至少 10 USD 才能避免极小单）
  - [ ] `DAILY_MAX_LOSS_PCT` 上限满足策略预期（如 <= 0.05）
  - [ ] `MAX_OPEN_POSITIONS >= 1`
- [ ] 关键控制位可用
  - [ ] 控制 API 生效：`pause_opening/reduce_only/emergency_stop`
  - [ ] `emergency_stop` 与 `reduce_only` 在 UI/接口可快速触发并可回退

## D. 运行一致性（P1）

- [ ] 状态面板口径一致
  - [ ] “执行模式”显示与 `DRY_RUN` 一致
  - [ ] 预算显示为 `tracked_notional_usd / available_notional_usd`
  - [ ] “可用预算”“槽位利用率”不再出现未定义字段
- [ ] 数据完整性
  - [ ] `positions/wallets/sources/orders/alerts` 字段存在且可为空但不缺字段
  - [ ] 控制状态变更（`updated_ts`）可追踪

## E. 运行可靠性（P1）

- [ ] 进程自愈
  - [ ] 启动脚本在无 launchctl 权限的机器可降级到直跑：`scripts/start_poly_stack.sh`
  - [ ] 8787 端口冲突时有清理与回退日志
- [ ] 资源与清理
  - [ ] 启动后 2 分钟内无频繁重启
  - [ ] 无残留僵尸监听（`lsof -nP -iTCP:8787 -sTCP:LISTEN`）

## F. 实盘前演练（P1）

- [ ] 低规模演练（纸面/模拟）
  - [ ] 先 `DRY_RUN=true` 跑 24 小时无异常
  - [ ] 模拟 `emergency_stop` 演练一次并恢复
- [ ] 切换 live 的红线控制
  - [ ] 切 `DRY_RUN=false` 前做 5 分钟干跑验证
  - [ ] 首单前确认真实地址与 funder 正确

## G. 监控告警（P2）

- [ ] 监控覆盖
  - [ ] API 健康（`/api/state`）
  - [ ] 错误计数（rejected/skip）
  - [ ] 日内损失率与槽位利用率
- [ ] 告警策略
  - [ ] 关键失败触发人通知（至少 Slack/邮件）

## H. 回退手册（SOP）

### 快速回退（5 分钟）
1. UI 停止开仓：进入控制面板点“暂停开仓”
2. 启动紧急停止：点“紧急退出”
3. 切回安全模式：设置 `DRY_RUN=true` 并重启服务
4. 停止服务
   - [ ] 直跑：`pkill -f polymarket_bot.web ; pkill -f polymarket_bot.daemon`
   - [ ] launchctl：`launchctl bootout gui/$(id -u)/ai.poly.web ; launchctl bootout gui/$(id -u)/ai.poly.bot`
5. 持仓核对：比对钱包当前仓位与 `/tmp/poly_runtime_data/state.json`

### 回退确认
- [ ] `make verify` 通过（环境侧）
- [ ] Web API 恢复到最近 1 次有效心跳
- [ ] `/tmp/poly_runtime_data/state.json` 可读取且 `control.emergency_stop`/`reduce_only` 正确落盘

## I. 上线发布标准

- [ ] 本清单 P0 全部通过，P1 全数通过，P2 至少 80%
- [ ] 记录“上线时刻、版本、配置 hash、上游数据源状态、钱包地址”三元组
- [ ] 决策人签字后进入 24 小时观察期
