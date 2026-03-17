# Polymarket Automated Trading System

A pragmatic, configurable auto-trader for Polymarket.

Default mode is `paper trading` (safe). It can be switched to live CLOB execution once credentials are configured.

## What It Does

- Polls target wallets from `WATCH_WALLETS`
- Detects new/increased positions (smart-money follow signal)
- Applies risk constraints
- Executes via `PaperBroker` (default) or `LiveClobBroker` (optional, needs `py-clob-client` + key config)

## Strategy (v1)

Signal: if a watched wallet opens or increases a position by at least `MIN_WALLET_INCREASE_USD`, emit a `BUY` signal for that token.

Wallet screening (Polymarket-native):
- Candidate wallets come from static seed list `WATCH_WALLETS` plus optional dynamic discovery from `WALLET_DISCOVERY_PATHS`.
- Discovery controls: `WALLET_DISCOVERY_ENABLED`, `WALLET_DISCOVERY_MODE`, `WALLET_DISCOVERY_PATHS` (recommend `/trades`), `WALLET_DISCOVERY_TOP_N`, `WALLET_DISCOVERY_MIN_EVENTS`, `WALLET_DISCOVERY_REFRESH_SECONDS`.
- A wallet is monitored only if current Polymarket active positions pass all filters: `MIN_WALLET_ACTIVE_POSITIONS`, `MIN_WALLET_UNIQUE_MARKETS`, `MIN_WALLET_TOTAL_NOTIONAL_USD`, `MAX_WALLET_TOP_MARKET_SHARE`.

Risk checks:
- Max risk per trade (`RISK_PER_TRADE_PCT`)
- Daily loss cap (`DAILY_MAX_LOSS_PCT`)
- Max open positions
- Price band guard (`MIN_PRICE` ~ `MAX_PRICE`)

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

The network smoke test also writes a JSONL record by default to `/tmp/poly_network_smoke.jsonl` (set by `NETWORK_SMOKE_LOG` env var).

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

## Live Trading Enablement

1. Install live dependency:

```bash
pip install -e '.[live]'
```

2. Set in `.env`:
- `DRY_RUN=false`
- `PRIVATE_KEY=...`
- `FUNDER_ADDRESS=...`

3. Start bot.

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

- This is a framework you can extend (sell logic, stop-loss, TP, portfolio netting, market filters).
- `LiveClobBroker` expects `py-clob-client` API compatibility; if the upstream SDK changes, adjust the order call in `src/polymarket_bot/brokers/live_clob.py`.
- No guarantee of profitability. Use strict risk limits.

## Monitoring Reports

- 30m 报告（持续观察，不触发交易参数修改）:
  - `make monitor-30m`
  - 默认产物: `/tmp/poly_monitor_30m_report.txt`
  - 脚本: `scripts/monitor_thresholds_30m.sh`

- 12h 报告（持续观察，不触发交易参数修改）:
  - `make monitor-12h`
  - 默认产物: `/tmp/poly_monitor_12h_report.txt`
  - 脚本: `scripts/monitor_thresholds_12h.sh`

- 同时运行两档报告（长期后台）:
  - 直接前台运行: `make monitor-reports`（默认 both）
  - 后台 stop/clear:
    - `make stop-monitor-reports`
    - `make monitor-scheduler-install` 写入 `LaunchAgent`（一次安装后可自动常驻）
      - 安装脚本会自动尝试 `launchd`，若无权限则自动降级为 `nohup` 后台常驻；
      - 如想完全手工可直接用 `make monitor-reports`.
    - `make monitor-scheduler-uninstall`
    - `make monitor-scheduler-status`

支持参数:
  - `MONITOR_MODE=30m|12h|both`
  - `ROTATE_KEEP=<保留日志文件数，默认 24>`

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
