# BR-002 Candidate Lifecycle Policy

## Scope

This runbook defines the BR-002 business rule tightening for candidate lifecycle shrinkage in restricted live rehearsal.

In scope:

- one lifetime window for every candidate
- hard expiration before decision / queue / execution
- explicit block reason and block layer for expired candidates
- state and metrics export for lifecycle expiration

Out of scope:

- BR-001 repeat-entry / same-wallet add policy
- exit chain changes
- sizing formula changes
- breaker / control-plane changes

## Single Lifetime Truth

- Candidate freshness is evaluated from one timestamp only: `created_ts`.
- Candidate lifetime is controlled by one runtime setting: `CANDIDATE_TTL_SECONDS`.
- Queue time, approval time, temporary cache age, or `updated_ts` must not be treated as the freshness truth.

## Effective Expiry

Effective expiry is:

1. `created_ts + CANDIDATE_TTL_SECONDS`
2. capped by market end when the market window ends earlier

This means a candidate never gains extra lifetime from:

- being refreshed
- being approved
- being queued
- being manually touched later

## Hard Lifecycle Rule

- If a candidate is past its effective expiry time, it must be discarded.
- A discarded candidate must not:
  - re-enter the pending queue
  - stay executable through approved queue
  - reach broker execution through manual / queue / replayed plan paths

## Required Reason / Layer

Expired candidates must expose:

- `block_reason=candidate_lifetime_expired`
- `block_layer`

Expected layers in BR-002:

- `candidate`
- `decision`
- `execution_precheck`

## Lifecycle Model

Exported lifecycle state is normalized to:

- `active`
- `expired_discarded`
- `executed`
- `skipped`

This lifecycle model is derived from persisted candidate status and must not allow ambiguous dual-state interpretation.

## Export Expectations

`/api/state` must expose lifecycle summary under candidate observability, including:

- expired discarded count
- block reasons
- block layers
- reason-layer counts

`/metrics` must expose:

- `polymarket_candidate_expired_discarded_count`
- `polymarket_candidate_blocked_total{reason_code="candidate_lifetime_expired", block_layer="..."}`

## Verification

- Unit/integration:
  - `PYTHONPATH=src .venv/bin/python -m unittest discover -s tests -p 'test_candidate_lifetime_expiration.py'`
  - `PYTHONPATH=src .venv/bin/python -m unittest discover -s tests -p 'test_runner_control.py'`
  - `PYTHONPATH=src .venv/bin/python -m unittest discover -s tests -p 'test_daemon_state.py'`
  - `PYTHONPATH=src .venv/bin/python -m unittest discover -s tests -p 'test_web_api.py'`
  - `PYTHONPATH=src .venv/bin/python -m unittest discover -s tests -p 'test_db.py'`
  - `PYTHONPATH=src .venv/bin/python -m unittest discover -s tests -p 'test_api_state_and_metrics_consistent.py'`
- Behavior verification:
  - `PYTHONPATH=src:tests .venv/bin/python scripts/verify_candidate_lifetime_expiration.py`
