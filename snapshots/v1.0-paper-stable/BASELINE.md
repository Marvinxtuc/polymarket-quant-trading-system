# v1.0-paper-stable

Frozen at: 2026-03-11 16:52:27 CST
Workspace: /Users/marvin.xa/Desktop/Polymarket

## Baseline Scope
- Runtime config snapshot: `.env`, `.env.example`
- Core strategy/runtime snapshot: `src/polymarket_bot/config.py`, `src/polymarket_bot/runner.py`, `src/polymarket_bot/main.py`

## Baseline Characteristics
- `MAX_SIGNALS_PER_CYCLE=1`
- `MAX_OPEN_POSITIONS=5`
- `RISK_PER_TRADE_PCT=0.005`
- `DAILY_MAX_LOSS_PCT=0.025`
- `TOKEN_ADD_COOLDOWN_SECONDS=300`
- `TOKEN_REENTRY_COOLDOWN_SECONDS=900`
- Congestion adaptive exit enabled:
  - `CONGESTED_UTILIZATION_THRESHOLD=0.8`
  - `CONGESTED_STALE_MINUTES=10`
  - `CONGESTED_TRIM_PCT=0.75`

## Restore
```bash
cd /Users/marvin.xa/Desktop/Polymarket
cp snapshots/v1.0-paper-stable/.env .env
cp snapshots/v1.0-paper-stable/.env.example .env.example
cp snapshots/v1.0-paper-stable/config.py src/polymarket_bot/config.py
cp snapshots/v1.0-paper-stable/runner.py src/polymarket_bot/runner.py
cp snapshots/v1.0-paper-stable/main.py src/polymarket_bot/main.py
```

## Verify
Compare current file hashes with `SHA256SUMS` in this folder.
