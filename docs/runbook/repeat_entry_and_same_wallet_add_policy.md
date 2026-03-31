# BR-001 Repeat Entry and Same-Wallet Add Policy

## Scope

This runbook defines the BR-001 business rule tightening for restricted live rehearsal:

- default block on duplicate entry for the same local `token_id`
- explicit same-wallet add whitelist only
- no buy-side resonance amplification
- exported block reason/layer for replay and review

## Single Matching Key

- `token_id` is the only duplicate-entry / add matching key in this stage.
- Do not mix:
  - `market_slug`
  - `condition_id`
  - candidate ids
  - temporary snapshot rows

If two signals refer to the same local token exposure, they must be evaluated as the same `token_id`.

## Single Local Position Truth

- The only truth source for “already holding this token locally” is runtime `positions_book`.
- Candidate status, approved queue state, and temporary account snapshots cannot override repeat-entry blocking.
- Any queue/approval path that reaches execution must re-check the same `positions_book` truth before BUY execution.

## Default Rule

- If `positions_book[token_id]` exists with positive local notional, a new BUY is blocked by default.
- This applies even if the new signal reaches:
  - candidate review
  - approved queue
  - execution precheck

## Same-Wallet Add Whitelist

Same-wallet add is allowed only when all three conditions are true:

1. signal wallet equals current position `entry_wallet`
2. `SAME_WALLET_ADD_ENABLED=true`
3. the wallet appears in `SAME_WALLET_ADD_ALLOWLIST`

If any condition is missing, BUY must stay blocked.

## Cross-Wallet Rule

- If another wallet already owns the local `token_id` position, a new wallet signal must not enlarge that position.
- Cross-wallet buy resonance cannot be used to justify additional entry size on an already-open local token.

## Buy Resonance Semantics

- Buy-side multi-wallet resonance is observe-only in BR-001.
- It may enrich candidate explanation or wallet summary text.
- It must not change:
  - BUY `observed_notional`
  - BUY `observed_size`
  - BUY `price_hint`
  - candidate action level (`buy_small`, `buy_normal`, `follow`)
  - execution-side sizing tendency

Exit-side `resonance_exit` is out of scope for BR-001.

## Required Reason Codes

Blocked repeat-entry paths must emit explicit reason codes:

- `repeat_entry_blocked_existing_position`
- `same_wallet_add_not_allowed`
- `cross_wallet_repeat_entry_blocked`

## Required Export Fields

Review surfaces must expose both:

- `block_reason`
- `block_layer`

Expected layers in BR-001:

- `candidate`
- `execution_precheck`
- `decision` (generic decision/risk layer when applicable)

## Verification

- Unit/integration:
  - `PYTHONPATH=src .venv/bin/python -m unittest discover -s tests -p 'test_wallet_follower.py'`
  - `PYTHONPATH=src .venv/bin/python -m unittest discover -s tests -p 'test_runner_control.py'`
  - `PYTHONPATH=src .venv/bin/python -m unittest discover -s tests -p 'test_web_api.py'`
  - `PYTHONPATH=src .venv/bin/python -m unittest discover -s tests -p 'test_daemon_state.py'`
- Behavior verification:
  - `PYTHONPATH=src:tests .venv/bin/python scripts/verify_repeat_entry_blocking.py`
