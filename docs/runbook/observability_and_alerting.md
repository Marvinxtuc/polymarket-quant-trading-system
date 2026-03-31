# Observability and Alerting Runbook (BLOCK-008)

This runbook defines the external observability loop for production safety signals.

## Scope

- Metrics endpoint: `/metrics` (Prometheus text format)
- State endpoint: `/api/state` (`observability` section)
- Both endpoints are projected from one request-local observability snapshot.
- `/metrics` is strictly read-only and has no write side effects.

## Request Snapshot Model

`build_handler()` now computes one request snapshot:

1. Load state from `state.json` and normalize through `_api_state_payload()`.
2. Attach `control` and `control_plane_security`.
3. Build one observability snapshot via `build_observability_snapshot(...)`.
4. Project:
   - `/api/state` -> JSON (`observability` field)
   - `/metrics` -> Prometheus text (`render_prometheus_metrics`)

Important:

- The same request does not re-fetch state for API vs metrics projections.
- `/metrics` never triggers:
  - heartbeat updates
  - runtime refresh
  - recovery probes
  - cache/file writes

## Heartbeat Semantics

- Heartbeat source of truth: `runner_heartbeat` in runtime state.
- Only the active runner can update heartbeat.
- Read-only web/standby/conflict instances must not update heartbeat.

Fields:

- `last_seen_ts`
- `last_cycle_started_ts`
- `last_cycle_finished_ts`
- `cycle_seq`
- `loop_status`
- `writer_active`

Staleness:

- Threshold: `observability_heartbeat_stale_seconds`
- Alert when `now - last_seen_ts > threshold`

## buy_blocked_duration_seconds Semantics

Source: runner-maintained `buy_blocked` state.

Start condition:

- BUY gate transitions to blocked (`_buy_gate_reason()` returns non-empty) and `since_ts` was 0.

Clear condition:

- BUY gate returns to allowed (`_buy_gate_reason()` empty), `since_ts` resets to 0 and duration resets to 0.

Duration:

- `duration_seconds = now - since_ts` when blocked.

Threshold alert:

- `observability_buy_blocked_alert_seconds`
- Triggers `buy_blocked_too_long` when blocked duration reaches threshold.

## Fixed Alert Code Whitelist

`alert_code` must come from a fixed whitelist only (no dynamic high-cardinality labels):

- `runner_heartbeat_stale` -> `page`
- `admission_fail_closed` -> `page`
- `reconciliation_fail` -> `page`
- `account_snapshot_stale` -> `warning`
- `event_stream_stale` -> `warning`
- `ledger_diff_exceeded` -> `page`
- `kill_switch_inflight` -> `warning`
- `kill_switch_manual_required` -> `page`
- `signer_unhealthy` -> `page`
- `writer_conflict_readonly` -> `page`
- `hot_wallet_cap_exceeded` -> `page`
- `buy_blocked_too_long` -> `warning`

The mapping above is fixed in code (`ALERT_CODE_TO_SEVERITY`) and used by docs/tests/scripts.

## Metrics (minimum set)

- `polymarket_runner_heartbeat_age_seconds`
- `polymarket_runner_heartbeat_stale`
- `polymarket_runner_writer_active`
- `polymarket_admission_opening_allowed`
- `polymarket_reconciliation_fail`
- `polymarket_account_snapshot_stale`
- `polymarket_event_stream_stale`
- `polymarket_ledger_diff_exceeded`
- `polymarket_kill_switch_manual_required`
- `polymarket_signer_healthy`
- `polymarket_writer_readonly_mode`
- `polymarket_hot_wallet_cap_ok`
- `polymarket_buy_blocked`
- `polymarket_buy_blocked_duration_seconds`
- `polymarket_alert_active{alert_code,severity}`

Label safety:

- `polymarket_alert_active` only allows labels:
  - `alert_code` (fixed whitelist)
  - `severity` (`page|warning`)
- No wallet/order_id/market slug/exception text labels.

## API Visibility (`/api/state`)

`/api/state` includes:

- `observability.generated_ts`
- `observability.heartbeat`
- `observability.admission`
- `observability.kill_switch`
- `observability.signer`
- `observability.writer`
- `observability.buy_blocked`
- `observability.active_alerts`
- `observability.metrics`

Evidence summary includes:

- `snapshot_age_seconds`
- `event_sync_age_seconds`
- `ledger_diff`
- `reconciliation_status`

## Failure Mode

If metrics derivation fails:

- `/api/state` still returns normalized runtime payload.
- `/metrics` request returns an explicit server error (no fake healthy response).
- Failure must be visible and not silently downgraded.

