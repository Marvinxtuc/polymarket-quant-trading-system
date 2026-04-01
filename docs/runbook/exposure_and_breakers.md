# Exposure and Breakers Runbook (BLOCK-009)

## Scope

BLOCK-009 implements and documents the runtime risk hard-gates around:

- exposure ledger limits
- wallet / portfolio / condition caps
- loss streak breaker
- intraday drawdown breaker
- restart persistence of breaker state
- `/api/state` exposure of risk breaker state

## Shared Units

- All exposure numbers are expressed in USD.
- BUY approval logic must convert every cap and ledger check into the same USD basis before comparing values.
- When sources disagree, USD-normalized runtime state is the reference format for docs, tests, and operator reporting.

## Cap Evaluation Model

BUY admission uses a single `RiskManager` decision fed by the persisted ledger/breaker projection.

The check order is fixed:

1. risk ledger / breaker state fault (`risk_ledger_fault`, `risk_breaker_state_invalid`)
2. active breakers (`loss_streak_breaker_active`, `intraday_drawdown_breaker_active`)
3. exposure caps (`wallet` -> `portfolio` -> `condition`)

If multiple reasons hit, `reason_codes` keeps all hits, and `primary_reason` follows the fixed priority above.

Condition cap semantics:

- when `PORTFOLIO_NETTING_ENABLED=true`, condition cap participates in the same BUY decision
- when `PORTFOLIO_NETTING_ENABLED=false`, condition cap is disabled (`cap_notional_usd=0`)

## Loss Streak Breaker

- Loss streak counts realized close PnL events only.
- `realized_pnl < 0` increments the counter by 1.
- `realized_pnl > 0` resets the counter to 0.
- `realized_pnl == 0` keeps the counter unchanged.
- Partial / split exits are counted per realized close event (each negative realized close increments once).
- A full state reset or explicit recovery reset clears the streak.
- When the configured streak threshold is reached, BUY is blocked until the breaker release condition is satisfied.

## Intraday Drawdown Breaker

- Intraday drawdown is measured against the configured day boundary (`RISK_BREAKER_TIMEZONE`), not a rolling 24h window.
- `equity_peak_usd` is tracked per day key.
- `intraday_drawdown_pct = (equity_peak_usd - equity_now_usd) / equity_peak_usd`.
- Day rollover resets drawdown state when `RISK_BREAKER_RESET_NEXT_DAY=true`.
- To avoid false positives on partial account snapshots, drawdown latch requires both:
  - drawdown over threshold, and
  - negative realized-loss evidence (`effective_daily_realized_pnl < 0`).
- If timezone/day-key resolution is invalid, breaker state becomes invalid and BUY fail-closes.

## Breaker Release Conditions

A breaker may clear only when:

- the triggering condition is no longer present
- the persisted breaker state is reloaded successfully
- the runtime export and internal state agree
- the operator has not left a manual latch in place

If any of those checks fail, the breaker stays latched.

Manual clear path:

- operator sets `clear_risk_breakers_requested_ts`
- runtime clears loss streak / drawdown breaker flags and re-evaluates on next cycle
- if upstream evidence is still unhealthy, breaker re-latches immediately

## Restart and Persistence

- Breaker state must survive restart.
- A restart does not imply a reset.
- If the state store or export is missing / inconsistent, the system should keep BUY blocked instead of guessing.

## Risk Fault Fail-Closed

Any of the following is treated as a fail-closed risk fault:

- breaker state cannot be loaded
- breaker state is inconsistent across runtime surfaces
- risk exports disagree with stored state
- day boundary cannot be resolved safely
- cap accounting cannot be normalized to USD

In those cases the operator view should show BUY as blocked until the risk state is repaired.

## Operator Verification

Expected validation focus:

- exposure ledger limit coverage
- wallet / condition cap coverage
- loss streak breaker BUY block coverage
- intraday drawdown breaker BUY block coverage
- restart persistence coverage
- `/api/state` risk breaker visibility

## `/api/state` Minimum Risk Fields

Operator-facing `risk_state` includes the minimum BLOCK-009 fields:

- exposure usage across all three scopes:
  - `wallet_exposure_usage_pct`
  - `portfolio_exposure_usage_pct`
  - `condition_exposure_usage_pct`
- breaker counters and current values:
  - `loss_streak_current`
  - `intraday_drawdown_current`
- latch and reasons:
  - `breaker_latched`
  - `reason_codes`

## Verification Hooks

- `PYTHONPATH=src .venv/bin/python scripts/verify_exposure_caps.py`
- `PYTHONPATH=src .venv/bin/python scripts/verify_loss_streak_and_drawdown_breakers.py`
- `bash scripts/gates/gate_block_item.sh BLOCK-009`
