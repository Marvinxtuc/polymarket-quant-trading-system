# Polymarket Personal Decision Terminal

A pragmatic, configurable personal trading terminal for Polymarket.

Default mode is `paper trading` + `manual` decision review. The system now queues wallet-follow opportunities as candidates first, then lets you `ignore / watch / buy_small / follow` before execution. It can still be switched to live CLOB execution once credentials are configured.

## What It Does

- Polls target wallets from `WATCH_WALLETS`
- Detects new/increased positions from wallet events with position-diff fallback
- Enriches them into a candidate queue with wallet score, spread/chase context, and suggested action
- Adds 5m / 30m momentum context from `prices-history` so candidates show trend instead of only current spread
- Persists candidates, wallet profiles, and journal entries in local SQLite
- Executes via `PaperBroker` (default) or `LiveClobBroker` (optional, needs `py-clob-client` + key config) after decision-mode gating

## Strategy (Candidate-first)

Candidate + signal flow:
- Primary path: read wallet `/trades` and/or `/activity` events (`WALLET_SIGNAL_SOURCE=trades|activity|hybrid`) and emit follow signals from fresh wallet trade events.
- Fallback path: if event data is missing or delayed, use active-position diffs so the bot still sees adds / trims / full exits.
- Warm-up behavior remains conservative: the first cycle primes event cursors and current wallet state, instead of replaying already-open exposure.
- Strategy internals now split into `detect_wallet_events() -> build_candidates() -> rank_candidates() -> generate_signals()`, so the UI can show why an idea is worth looking at before it becomes an execution signal.

Wallet screening (Polymarket-native):
- Candidate wallets come from static seed list `WATCH_WALLETS` plus optional dynamic discovery from `WALLET_DISCOVERY_PATHS`.
- Discovery controls: `WALLET_DISCOVERY_ENABLED`, `WALLET_DISCOVERY_MODE`, `WALLET_DISCOVERY_PATHS` (recommend `/trades`), `WALLET_DISCOVERY_TOP_N`, `WALLET_DISCOVERY_MIN_EVENTS`, `WALLET_DISCOVERY_REFRESH_SECONDS`.
- Event-signal controls: `WALLET_SIGNAL_SOURCE`, `WALLET_SIGNAL_LOOKBACK_SECONDS`, `WALLET_SIGNAL_PAGE_SIZE`, `WALLET_SIGNAL_MAX_PAGES`.
- A wallet is monitored only if current Polymarket active positions pass all filters: `MIN_WALLET_ACTIVE_POSITIONS`, `MIN_WALLET_UNIQUE_MARKETS`, `MIN_WALLET_TOTAL_NOTIONAL_USD`, `MAX_WALLET_TOP_MARKET_SHARE`.

Risk checks:
- Max risk per trade (`RISK_PER_TRADE_PCT`)
- Daily loss cap (`DAILY_MAX_LOSS_PCT`)
- Max open positions
- Condition-level portfolio netting cap (`PORTFOLIO_NETTING_ENABLED`, `MAX_CONDITION_EXPOSURE_PCT`)
- Price band guard (`MIN_PRICE` ~ `MAX_PRICE`)

Decision terminal:
- `DECISION_MODE=manual|semi_auto|auto`
- Candidate queue persisted in `CANDIDATE_DB_PATH`
- Candidate actions + journal exposed via `/api/candidates`, `/api/candidate/action`, `/api/journal`, `/api/wallet-profiles`, `/api/mode`
- Runtime stats / archive / export endpoints: `/api/stats`, `/api/archive`, `/api/export`
- Optional A-grade notifications via `CANDIDATE_NOTIFICATION_*`, `NOTIFY_WEBHOOK_URL(S)`, and Telegram Bot settings
- Frontend now includes candidate detail, notifier summary, and archive/export panels

## Quick Start

1. Create virtual env and install:

```bash
cd /Users/marvin.xa/Desktop/Polymarket
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
```

Optional: use Makefile shortcuts:

```bash
make venv
make install
make test
```

2. Configure env:

```bash
cp .env.example .env
# edit .env
```

Optional env check:

```bash
python scripts/check_env.py
make env-check
```

3. Run one cycle (recommended first):

```bash
polybot --once
```

Or via Makefile:

```bash
make run-once
```

4. Run continuous:

```bash
polybot
```

Or via Makefile:

```bash
make run
```

5. Run tests and runtime verification:

```bash
make test
make verify-stack   # checks an already-running local stack
make verify         # env-check + tests + stack verification
make one-click      # network + tests + restart stack + runtime verify
make network-smoke  # check polymarket site/data-api/clob endpoint accessibility and geo-block hints
```

The network smoke test also writes a JSONL record by default to `/tmp/poly_network_smoke.jsonl` (set by `NETWORK_SMOKE_LOG` env var). Live startup self-checks now read the latest record from that log, so a fresh `make network-smoke` is part of the live handoff path rather than a one-off manual probe.

Before going live, run the readiness checklist:

```bash
cat preprod_readiness_checklist.md
```

For first 4h / 12h live monitoring and rollback thresholds:

```bash
cat preprod_operations_playbook.md
```

For a one-page 12h quick post-run template:

```bash
cat preprod_operations_playbook.md | sed -n '/## 七、12h 极简复盘（快速版）/,/$/p'
```

Quick stop/restart helper:

```bash
make stop-stack
```

## Dashboard + Bot (One-Click)

- Frontend: `frontend/` (this repo)
- Runtime API: `GET /api/state` served by `polymarket_bot.web`
- Decision / export APIs: `GET /api/candidates`, `GET /api/stats`, `GET /api/archive`, `GET /api/export`
- Bot daemon: `polymarket_bot.daemon` writes runtime state to `/tmp/poly_runtime_data/state.json`
- Pre-production readiness checklist: `preprod_readiness_checklist.md`

Desktop launcher:
- `一键 poly.app` is intended to start this repository through the SOP entry `make one-click`
  (`network-smoke` + `run unit tests` + `start stack` + `verify stack`), then opens
  `http://127.0.0.1:8787`.
- If `一键 poly.app` still has no visible reaction on your machine, run this fallback
  (double-click friendly) shortcut:
  ```bash
  make desktop-command
  ```
  It creates `一键 poly.command` (default under `~/Desktop`, or `POLY_DESKTOP_DIR` if set) which invokes
  `scripts/run_one_click_launcher.sh` directly.
- The GUI command has on-screen output, so you can see why it exits:
  - success: prints startup message then opens dashboard
  - failure: prints the latest error lines and waits for Enter before closing.
- If current network is geo-blocked, use no-network mode:
  ```bash
  POLY_SKIP_NETWORK_SMOKE=1 make desktop-command
  ```
  This generates the same launcher shortcut but marks it to skip the smoke check step at runtime.
- If you still need `.app` to be rewritten after system cache/path changes, run:
  ```bash
  make repair-desktop-launcher
  ```
  This regenerates:
  - `~/Desktop/一键 poly.app/Contents/MacOS/start_app`
  - `~/Desktop/一键 poly.command`
- Web UI is on `http://127.0.0.1:8787`.
- `make start-stack` restarts the local stack and now verifies that `/api/state` is fresh.
- `make full-validate` now runs a quick end-to-end acceptance pass: restart stack, regenerate 30m/12h monitor JSON in `0s` quick mode, refresh EOD reconciliation through `POST /api/operator`, call the three report APIs, and run replay + replay-calibration against current runtime artifacts.
- Git autosync is available for local development: `make git-autosync-start` runs a polling watcher that automatically `git add -A`, `git commit`, and `git push origin HEAD` after file changes settle; `make git-autosync-install` installs the same watcher under `launchd`.
- Safety default: autosync refuses to start on a dirty worktree unless you explicitly set `GIT_AUTOSYNC_ALLOW_DIRTY_START=1`, so you do not accidentally auto-push the entire current worktree.
- Runtime artifacts live under `/tmp/poly_git_autosync/`, and `make git-autosync-status` shows the current watcher status plus the last sync result.

## Live Trading Enablement

1. Install live dependency:

```bash
pip install -e '.[live]'
```

2. Set in `.env`:
- `DRY_RUN=false`
- `CLOB_SIGNATURE_TYPE=0` for standard EOA wallets (`1` for Magic/email wallets, `2` for proxy/browser wallets)
- `PRIVATE_KEY=...`
- `FUNDER_ADDRESS=...`
- Optional user stream tuning: `USER_STREAM_ENABLED=true`, `USER_STREAM_URL=wss://ws-subscriptions-clob.polymarket.com/ws/user`
- Live admission gate: set `LIVE_ALLOWANCE_READY=true`, `LIVE_GEOBLOCK_READY=true`, and `LIVE_ACCOUNT_READY=true` only after you have manually confirmed those preconditions; `LIVE_NETWORK_SMOKE_MAX_AGE_SECONDS` controls how old the latest smoke log may be before startup blocks.

3. Start bot.

Live execution note:
- `LiveClobBroker` now treats posted/live orders as `pending` until broker position reconciliation confirms the fill, instead of immediately booking them as fully filled in memory.
- Before submitting a live order, the broker now fetches CLOB `book` + `midpoint`, rounds price to valid tick size, avoids crossing the opposite quote for resting GTC orders, and rejects orders below `min_order_size`.
- Live startup gate now treats allowance/geoblock/account readiness as explicit pass/fail confirmations instead of warnings, so missing acknowledgements block `startup_ready`.
- Wallet-follow signals now default to `hybrid` mode, which prefers `/trades` / `/activity` events and only falls back to position diffs when event data is unavailable.
- Signal, pending-order, and runtime position records now carry `condition_id`, and buy-side condition-level portfolio netting is enabled by default so repeated wallet signals on the same event share one exposure budget.
- Runtime now keeps an append-only ledger at `LEDGER_PATH`, restores same-day realized PnL from that ledger on restart, and stores per-position `cost_basis_notional` in runtime state so sell-side realized PnL can survive restarts.
- In live mode, the bot also polls Polymarket accounting snapshot + current-day closed positions (`ACCOUNT_SYNC_REFRESH_SECONDS`) to surface `equity`, `cash_balance`, `positions_value`, and a conservative `broker_closed_pnl_today` risk floor.
- On startup, live mode will also try to recover still-open exchange orders from the broker so pending state is rebuilt from exchange truth before falling back to the last runtime snapshot.
- Pending live orders now reconcile against both broker order status and recent authenticated trade fills, so partial fills can update runtime positions without waiting for the next positions snapshot to fully catch up.
- Live execution events now also preserve structured preflight market context (`best_bid`, `best_ask`, `midpoint`, `tick_size`, `market_spread_bps`) so replay and shadow analysis can use real sample spreads.
- Live broker now exposes a polling-based own-order event stream abstraction (`status` + `fill` events), so runner can consume incremental execution updates before the next full runtime reconcile.
- When `websocket-client` is installed, live mode will also open Polymarket's authenticated user channel (`USER_STREAM_URL`) and buffer `order` / `trade` events in the broker; if the stream dependency is missing or the connection drops, the bot automatically falls back to the existing polling-based reconcile path.
- Runtime snapshots now persist the last broker-event watermark, so restart recovery can resume incremental reconcile without widening the replay window back to zero every time.
- Trader startup now records a readiness checklist into the event log, ledger, and daemon state. In live mode that checklist includes broker capability checks plus the latest `NETWORK_SMOKE_LOG` result, and will mark the process not-ready if the last smoke run reported a block or endpoint failure, the smoke record is stale, or the explicit live admission flags are not set.
- Daemon state now also exposes an execution reconciliation summary, including `internal_vs_ledger_diff`, pending-order staleness, snapshot age, broker reconcile age, and broker floor gap, so monitoring can distinguish “strategy looked quiet” from “execution facts have drifted”.

## Environment Notes

- Keep real secrets in `.env` and never commit them.
- `.env.example` is the safe template; update it when you add new settings.
- For paper trading, you can leave `PRIVATE_KEY` and `FUNDER_ADDRESS` empty.
- For live trading, `PRIVATE_KEY` and `FUNDER_ADDRESS` are required.

Before switching to live-like testing, run:

```bash
make network-smoke
```

You can also override log destination:

```bash
NETWORK_SMOKE_LOG=/tmp/poly_network_smoke.jsonl make network-smoke
```

## Notes

- This is still a framework you can extend further (stop-loss, TP, deeper netting, market filters).
- `LiveClobBroker` expects `py-clob-client` API compatibility; if the upstream SDK changes, adjust the order call in `src/polymarket_bot/brokers/live_clob.py`.
- No guarantee of profitability. Use strict risk limits.

## Monitoring Reports

- 30m 报告（持续观察，不触发交易参数修改）:
  - `make monitor-30m`
  - 默认产物: `/tmp/poly_monitor_30m_report.txt`
  - JSON 产物: `/tmp/poly_monitor_30m_report.json`
  - 脚本: `scripts/monitor_thresholds_30m.sh`
  - 现在会同时读取 daemon `state.json` 中的 `startup` / `reconciliation` 摘要，报告里会额外标出账本漂移、pending 单陈旧和同步年龄。

- 12h 报告（持续观察，不触发交易参数修改）:
  - `make monitor-12h`
  - 默认产物: `/tmp/poly_monitor_12h_report.txt`
  - JSON 产物: `/tmp/poly_monitor_12h_report.json`
  - 脚本: `scripts/monitor_thresholds_12h.sh`
  - 同样会把 `startup_ready` 和 `reconciliation.status` 纳入最终建议，不再只看日志计数。

- 独立日终对账报告:
  - `make reconciliation-report`
  - 默认产物: `/tmp/poly_reconciliation_eod_report.txt`
  - JSON 产物: `/tmp/poly_reconciliation_eod_report.json`
  - 脚本: `scripts/generate_reconciliation_report.py`
  - 报告会汇总当日 ledger fill/account_sync/startup_checks、state 里的 reconciliation 摘要，以及按 `source` / `side` 的成交分解。

- 全流程验收报告:
  - `make full-validate`
  - 默认产物: `/tmp/poly_full_flow_validation_report.txt`
  - JSON 产物: `/tmp/poly_full_flow_validation_report.json`
  - 脚本: `scripts/full_flow_validate.py`
  - 默认会先重启本地 stack，再用 `0s` quick window 重建 monitor 报告、刷新 EOD 对账、验证 `/api/state` + `/api/monitor/*` + `/api/reconciliation/eod`，最后跑一遍 runtime replay / replay-calibration。
  - 报告会把“流程是否打通”与“当前 readiness 是 READY / OBSERVE / ESCALATE / BLOCK”分开呈现，方便先验收链路，再看运营门禁。

- 同时运行两档报告（长期后台）:
  - 直接前台运行: `make monitor-reports`（默认 both）
  - 后台 stop/clear:
    - `make stop-monitor-reports`
    - `make monitor-scheduler-install` 写入 `LaunchAgent`（一次安装后可自动常驻）
      - 安装脚本会自动尝试 `launchd`，若无权限则自动降级为 `nohup` 后台常驻；
      - 如在 Desktop/受限目录里想直接绕过 `launchd`，可用 `MONITOR_FORCE_NOHUP=1 make monitor-scheduler-install`
      - 如想完全手工可直接用 `make monitor-reports`.
    - `make monitor-scheduler-uninstall`
    - `make monitor-scheduler-status`

支持参数:
  - `MONITOR_MODE=30m|12h|both`
  - `ROTATE_KEEP=<保留日志文件数，默认 24>`
  - `MONITOR_DAEMON_LOG=<daemon 日志路径，默认 /tmp/poly_runtime_data/poly_bot.log>`
  - API 可直接读取:
    - `/api/monitor/30m`
    - `/api/monitor/12h`
    - `/api/reconciliation/eod`
    - `POST /api/operator` with `{"command":"generate_reconciliation_report"}` 可直接刷新 EOD 对账产物
    - `POST /api/operator` with `{"command":"clear_stale_pending"}` 会请求 runner 在下一轮清理已超时的 pending 单
  - Dashboard 现在会直接消费这 3 个 JSON 接口，显示 monitor 最终建议、EOD 对账状态和成交分解，不再只展示 `/api/state`。
  - 顶部还会有一层 operator gate banner，把 `READY / OBSERVE / ESCALATE / BLOCK` 结论直接顶到最上面，并在异常时高亮紧急退出按钮。
  - Gate banner 还会自动生成建议动作清单，例如 `make network-smoke`、处理 stale pending、核对 ledger drift，减少 live 排障时的来回切换。
  - 建议动作支持直接交互：可以复制排障命令、跳到订单/监控面板，或直接打开 `/api/monitor/*`、`/api/reconciliation/eod` JSON。
  - 其中 EOD 对账现在也支持从 dashboard 直接触发刷新，不必手工回终端执行。
  - Dashboard 还新增了“执行诊断明细”面板，直接展开 startup checks、reconciliation facts 和 monitor/EOD 重点问题，作为 operator gate 的下钻视图。
  - 诊断面板现在还会展开活跃 pending/stale 订单，并支持点击 startup check 或 pending order 查看结构化焦点详情。
  - `clear_stale_pending` 的最近一次执行结果也会回写到 runtime state，并在诊断面板里显示 `REQUESTED / CLEARED / NOOP` 状态。

- 10 小时实盘前演练（paper）：
  - `make rehearse-10h`
  - 兼容旧入口: `make rehearse-12h`（已切换为 10h）
  - 每小时自动写 1 条 checkpoint
  - 结果落盘: `/tmp/poly_10h_paper_rehearsal.txt`
  - 运行日志: `/tmp/poly_10h_paper_rehearsal.log`
  - 查看最新进度: `make rehearse-progress`

- 轻量回放 / 参数校准：
  - `make replay`
  - `make replay-calibrate`
  - 直接 JSON 输出: `.venv/bin/python scripts/replay_calibration.py --json`
  - 按题材切片: `.venv/bin/python scripts/replay_calibration.py --topic 加密`
  - 列出钱包池版本: `.venv/bin/python scripts/replay_calibration.py --list-wallet-pools`
  - 按钱包池版本切片: `.venv/bin/python scripts/replay_calibration.py --wallet-pool 1a2b3c4d`
  - 自定义场景文件: `.venv/bin/python scripts/replay_calibration.py --scenario-file /path/to/scenarios.json`
  - 现在会同时输出 `gross_cashflow` 和 `net_cashflow`，并支持用 `REPLAY_TAKER_FEE_BPS`、`REPLAY_ENTRY_SLIPPAGE_BPS`、`REPLAY_EXIT_SLIPPAGE_BPS`、`REPLAY_FEE_KEYWORDS` 对 fee-enabled 市场做费用/滑点敏感性校准
  - 还支持 spread-aware 滑点参数：`REPLAY_ENTRY_SPREAD_MULTIPLIER`、`REPLAY_EXIT_SPREAD_MULTIPLIER`、`REPLAY_EDGE_PRICE_PENALTY_BPS`
  - replay 现在也会纳入 live reconcile 产生的 `order_reconciled` / `order_partial_fill` 样本
