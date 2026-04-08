# Order Idempotency Runbook (BLOCK-002)

## Scope
BLOCK-002 only handles durable idempotency for BUY submission/retry/recovery:
- persistent `claim_or_load_intent()` replaces in-memory dedupe as final authority
- `strategy_order_uuid` maps one strategy intent to one broker order lifecycle
- restart and duplicate executor paths must not create a second intent or second send

## Single Source of Truth
- table: `order_intents` in `state_store` (SQLite)
- unique key: `idempotency_key`
- `idempotency_key` input = `strategy_name + signal_source + signal_fingerprint + token + side + extra(wallet,condition,notional,signal_bucket) + salt`
- in-process TTL dedupe is rate-limit telemetry only; it never decides final idempotency

## Claim Contract
`StateStore.claim_or_load_intent()` returns exactly:
- `CLAIMED_NEW`
- `EXISTING_NON_TERMINAL`
- `EXISTING_TERMINAL`
- `STORAGE_ERROR`

This call is single-transaction (`BEGIN IMMEDIATE`) and enforces one persistent intent row per idempotency key.
The implementation does not do a split "read then insert" across transactions: lookup/insert/commit happens inside the same transaction, and unique-key conflicts are converted into `EXISTING_NON_TERMINAL`/`EXISTING_TERMINAL`.

## Order State Machine
`NEW -> SENDING -> ACKED_PENDING|PARTIAL|FILLED|CANCEL_REQUESTED|CANCELED|REJECTED|FAILED`

Additional recovery states:
- `ACK_UNKNOWN`: broker side-effect uncertain; must probe, cannot blind resend
- `MANUAL_REQUIRED`: non-terminal blocking state; operator close-out required before any new intent on same key

Rules:
- `NEW` means claimed but not sent yet
- `SENDING` means send critical section entered (side effect may exist)
- recovery for `NEW`: it can re-enter send path only through CAS `NEW -> SENDING`
- recovery for `SENDING`: it must probe broker/open-order evidence first; it cannot pass `NEW -> SENDING` CAS directly
- any non-terminal existing intent blocks new intent creation
- send path requires CAS transition `NEW -> SENDING`; if CAS fails, skip send

## ACK_UNKNOWN Recovery Guardrails
- recovery window: `ACK_UNKNOWN_RECOVERY_WINDOW_SECONDS`
- max probes: `ACK_UNKNOWN_MAX_PROBES`
- over limit in-window: promote to `MANUAL_REQUIRED` (no auto new order)
- `MANUAL_REQUIRED` is blocking non-terminal and keeps the same strategy intent locked until manual intervention
- broker-open probe hit: set `ACKED_PENDING` and keep same `strategy_order_uuid`

## Submit-Unknown Contract
When live submit returns success without a durable broker order id, the system must not treat that path as a normal posted order.

Required runtime payload fields:
- `pending_class=submit_unknown`
- `submit_digest`
- `submit_digest_version=sdig-v1`
- `submitted_price`
- `submitted_size`
- `tick_size`
- `unknown_submit_first_seen_ts`
- `unknown_submit_probe_count`
- `probe_confidence`
- `probe_basis`
- `manual_required_reason`

Evidence classes:
- `strong`
  - broker order id resolves directly, or exactly one broker record matches token/side/price/size/time window
  - action: recover same intent to `ACKED_PENDING|PARTIAL|FILLED`; never create a second intent
- `weak`
  - only `submit_digest` or incomplete broker evidence exists
  - action: keep same intent blocked in `ACK_UNKNOWN`; continue bounded probe loop only
- `none`
  - no stable broker anchor exists
  - action: promote to `MANUAL_REQUIRED` with explicit reason; auto resend remains forbidden

Probe basis values:
- `broker_order_id`
- `unique_broker_record_match`
- `ambiguous_broker_record_match`
- `submit_digest_only`
- `no_match`

Manual-required reasons:
- `submit_unknown_no_anchor`
- `submit_unknown_probe_exhausted`
- `submit_unknown_ambiguous_match`
- `submit_unknown_conflicting_evidence`

Operator rule:
- `MANUAL_REQUIRED` means the same idempotency key is still locked.
- Do not retry by forcing a new BUY on the same signal key until the broker/order truth is manually reconciled and the blocking intent is closed out.

## Behavior Verification
Run tests:
```bash
PYTHONPATH=src .venv/bin/python -m unittest discover -s tests -p "test_idempotent_order_submission.py"
PYTHONPATH=src .venv/bin/python -m unittest discover -s tests -p "test_duplicate_executor_same_signal.py"
PYTHONPATH=src .venv/bin/python -m unittest discover -s tests -p "test_timeout_retry_reuses_same_intent.py"
PYTHONPATH=src .venv/bin/python -m unittest discover -s tests -p "test_restart_does_not_duplicate_orders.py"
PYTHONPATH=src .venv/bin/python -m unittest discover -s tests -p "test_unknown_submit_contract.py"
```

Run behavior scripts:
```bash
PYTHONPATH=src:tests .venv/bin/python scripts/verify_idempotent_submission.py
PYTHONPATH=src:tests .venv/bin/python scripts/verify_restart_no_duplicate_order.py
```

The restart script must prove:
- broker already accepted but local ACK unknown
- no second intent row
- no second send path (`NEW -> SENDING` CAS fails for recovered non-NEW status)
- same `strategy_order_uuid` reused during recovery probe
