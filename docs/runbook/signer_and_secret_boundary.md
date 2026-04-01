# Signer & Secret Boundary (BLOCK-007)

## Scope

BLOCK-007 only covers signer/secret boundary hardening for live funds:

- live mode raw private key rejection
- external signer-only signing path
- startup identity consistency checks
- hot wallet cap checks (startup + runtime)
- minimal signer security state export

It does not change control-plane auth, alerts, idempotency, or single-writer design.

## Security Model

### Live mode (`DRY_RUN=false`)

- `PRIVATE_KEY` is forbidden and treated as a policy violation.
- startup must resolve a live secret bundle from:
  - `FUNDER_ADDRESS`
  - `SIGNER_URL`
  - `CLOB_API_KEY`
  - `CLOB_API_SECRET`
  - `CLOB_API_PASSPHRASE`
- missing/invalid signer or API creds is fail-closed.
- trading process submits signing payload to signer client and only receives minimal `signed_order`.
- signer response containing secret-like fields (for example `private_key`, `seed`, `mnemonic`, `api_secret`) is rejected.

### Paper mode (`DRY_RUN=true`)

- signer boundary is not required.
- existing paper broker behavior remains unchanged.

## Identity Consistency Checks

Startup requires signer identity binding to pass:

- signer identity == funder identity
- API creds binding identity == funder identity
- broker identity (funder) present

Any mismatch is fail-closed before live broker trading loop.

## Hot Wallet Cap Policy

`LIVE_HOT_WALLET_BALANCE_CAP_USD` (>=0):

- startup check: if live equity exceeds cap, startup check fails.
- runtime check: if equity later exceeds cap, runner latches recovery conflict (`HOT_WALLET_CAP_EXCEEDED`), blocks BUY via admission gate, and marks signer security reason code `hot_wallet_cap_exceeded`.

Cap measurement is consistent in code/tests/docs:

- primary: `equity_usd`
- fallback: `cash_balance_usd + positions_value_usd`

## Signer Status Export (minimal)

`/api/state.signer_security` and runtime export include only minimal booleans/summary fields.
Web enforces an allowlist in `_api_state_payload`, so non-allowlisted keys (for example signer URL / secret path / token) are removed:

- `live_mode`
- `signer_required`
- `signer_mode`
- `signer_healthy`
- `signer_identity_matched`
- `api_identity_matched`
- `broker_identity_matched`
- `raw_key_detected`
- `api_creds_configured`
- `hot_wallet_cap_enabled`
- `hot_wallet_cap_ok`
- `hot_wallet_cap_limit_usd`
- `hot_wallet_cap_value_usd`
- `reason_codes`

Do not export raw token/key/seed, signer URL topology details, or secret source internals.

## Signer Decision Table

- live + `PRIVATE_KEY` present -> startup blocked (`raw_private_key_forbidden_live`), no signer/broker init.
- live + signer missing/invalid -> startup blocked (`signer_url_missing` / `signer_mode_invalid`).
- live + signer health check exception -> startup blocked (`signer_unreachable` / `signer_health_*`).
- live + signer unhealthy -> startup blocked (`signer_unhealthy`).
- live + signer identity mismatch -> startup blocked (`signer_identity_mismatch` / `api_identity_mismatch`).
- runtime signer submit failure -> execution rejects and marks `security_fail_close`, runner latches recovery conflict, BUY remains blocked until manual recovery.

## Verification

Run:

```bash
PYTHONPATH=src:tests .venv/bin/python -m unittest discover -s tests -p "test_live_mode_rejects_raw_private_key.py"
PYTHONPATH=src:tests .venv/bin/python -m unittest discover -s tests -p "test_signer_required_in_live_mode.py"
PYTHONPATH=src:tests .venv/bin/python -m unittest discover -s tests -p "test_signer_failure_blocks_startup.py"
PYTHONPATH=src:tests .venv/bin/python -m unittest discover -s tests -p "test_signer_signs_without_exposing_key.py"
PYTHONPATH=src:tests .venv/bin/python -m unittest discover -s tests -p "test_hot_wallet_balance_cap_enforced.py"
PYTHONPATH=src:tests .venv/bin/python scripts/verify_signer_required_live.py
PYTHONPATH=src:tests .venv/bin/python scripts/verify_no_raw_key_in_live_mode.py
bash scripts/gates/gate_block_item.sh BLOCK-007
```
