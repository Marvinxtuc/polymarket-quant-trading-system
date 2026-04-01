# v1.1-paper-stable

Frozen at: 2026-03-13 09:45:00 CST
Workspace: ~/Desktop/Polymarket

## Baseline Scope
- Runtime config snapshot: `.env`, `.env.example`
- Core runtime snapshot: `src/polymarket_bot/config.py`, `src/polymarket_bot/main.py`, `src/polymarket_bot/runner.py`, `src/polymarket_bot/risk.py`, `src/polymarket_bot/daemon.py`, `src/polymarket_bot/web.py`
- Frontend mapping snapshot: `frontend/index.html`, `frontend/app.js`

## Baseline Characteristics
- `BANKROLL_USD=15000`
- `RISK_PER_TRADE_PCT=0.005`
- `DAILY_MAX_LOSS_PCT=0.025`
- `MAX_OPEN_POSITIONS=8`
- `MAX_SIGNALS_PER_CYCLE=1`
- `TOKEN_ADD_COOLDOWN_SECONDS=120`
- `TOKEN_REENTRY_COOLDOWN_SECONDS=900`
- `POLL_INTERVAL_SECONDS=60`
- `WALLET_DISCOVERY_MODE=replace`

## Health Gate Evidence
- 30m tuned window passed all 3 gates:
  - `SKIP(max open)/EXEC = 0.214`
  - `TIME_EXIT_CLOSE/EXEC = 0.500`
  - `SKIP(token add cooldown)/EXEC = 0.214`
- 1h stability window passed all 3 gates:
  - `SKIP(max open)/EXEC = 0.241`
  - `TIME_EXIT_CLOSE/EXEC = 0.759`
  - `SKIP(token add cooldown)/EXEC = 0.207`

## Restore
```bash
cd ~/Desktop/Polymarket
cp snapshots/v1.1-paper-stable/.env .env
cp snapshots/v1.1-paper-stable/.env.example .env.example
cp snapshots/v1.1-paper-stable/config.py src/polymarket_bot/config.py
cp snapshots/v1.1-paper-stable/main.py src/polymarket_bot/main.py
cp snapshots/v1.1-paper-stable/runner.py src/polymarket_bot/runner.py
cp snapshots/v1.1-paper-stable/risk.py src/polymarket_bot/risk.py
cp snapshots/v1.1-paper-stable/daemon.py src/polymarket_bot/daemon.py
cp snapshots/v1.1-paper-stable/web.py src/polymarket_bot/web.py
cp snapshots/v1.1-paper-stable/index.html frontend/index.html
cp snapshots/v1.1-paper-stable/app.js frontend/app.js
```

## Verify
Compare current file hashes with `SHA256SUMS` in this folder.
