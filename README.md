# Polymarket Automated Trading System

A pragmatic, configurable auto-trader for Polymarket.

Default mode is `paper trading` (safe). It can be switched to live CLOB execution once credentials are configured.

## What It Does

- Polls target wallets from `WATCH_WALLETS`
- Detects new/increased positions (smart-money follow signal)
- Applies risk constraints
- Executes via:
  - `PaperBroker` (default)
  - `LiveClobBroker` (optional, needs `py-clob-client` + key config)

## Strategy (v1)

Signal: if a watched wallet opens or increases a position by at least `MIN_WALLET_INCREASE_USD`, emit a `BUY` signal for that token.

Wallet screening (Polymarket-native):
- Candidate wallets come from:
  - static seed list `WATCH_WALLETS`
  - optional dynamic discovery from `WALLET_DISCOVERY_PATHS`
- Discovery controls:
  - `WALLET_DISCOVERY_ENABLED=true|false`
  - `WALLET_DISCOVERY_MODE=union|replace`
  - `WALLET_DISCOVERY_PATHS` (recommend `/trades`)
  - `WALLET_DISCOVERY_TOP_N`
  - `WALLET_DISCOVERY_MIN_EVENTS`
  - `WALLET_DISCOVERY_REFRESH_SECONDS` (cache refresh interval, e.g. `900`)
- A wallet is monitored only if current Polymarket active positions pass all filters:
  - `MIN_WALLET_ACTIVE_POSITIONS`
  - `MIN_WALLET_UNIQUE_MARKETS`
  - `MIN_WALLET_TOTAL_NOTIONAL_USD`
  - `MAX_WALLET_TOP_MARKET_SHARE`

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

2. Configure env:

```bash
cp .env.example .env
# edit .env
```

3. Run one cycle (recommended first):

```bash
polybot --once
```

4. Run continuous:

```bash
polybot
```

## Dashboard + Bot (One-Click)

- Frontend: `frontend/` (this repo)
- Runtime API: `GET /api/state` served by `polymarket_bot.web`
- Bot daemon: `polymarket_bot.daemon` writes runtime state to `/tmp/poly_runtime_data/state.json`

Desktop launcher:
- `一键 poly.app` now starts this repository stack via:
  - `/Users/marvin.xa/Desktop/Polymarket/scripts/start_poly_stack.sh`
  - web on `http://127.0.0.1:8787`

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

## Notes

- This is a framework you can extend (sell logic, stop-loss, TP, portfolio netting, market filters).
- `LiveClobBroker` expects `py-clob-client` API compatibility; if the upstream SDK changes, adjust the order call in `src/polymarket_bot/brokers/live_clob.py`.
- No guarantee of profitability. Use strict risk limits.
