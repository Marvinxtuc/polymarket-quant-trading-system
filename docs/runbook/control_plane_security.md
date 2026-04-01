# Control Plane Security (BLOCK-006)

## Scope

This runbook covers control-plane write API hardening only:

- token auth for write routes
- source-address policy and trusted proxy handling
- read/write route boundary
- write audit events
- `/api/state` security status export

It does not change strategy/risk logic.

## Security Model

### Read-only routes (GET)

Read API access is path + method whitelisted:

- `GET /api/state`
- `GET /api/control`
- `GET /api/monitor/30m`
- `GET /api/monitor/12h`
- `GET /api/reconciliation/eod`
- `GET /api/blockbeats`
- `GET /api/candidates`
- `GET /api/candidates/<candidate_id>`
- `GET /api/wallet-profiles`
- `GET /api/journal`
- `GET /api/stats`
- `GET /api/archive`
- `GET /api/export`
- `GET /api/mode`

Any other `GET /api/*` route is rejected.

Read-only mode keeps these handlers side-effect free: no state refresh writes, no public export writes,
no recovery probes, and no BlockBeats pull refresh.

### Write routes (POST)

Write API access is path + method whitelisted:

- `POST /api/control`
- `POST /api/operator`
- `POST /api/candidate/action`
- `POST /api/mode`
- `POST /api/journal/note`
- `POST /api/wallet-profiles/update`

Any other `POST /api/*` route is rejected.

## Single Write Availability Verdict

`write_api_available` is the only write-opening truth, reused by:

- startup write-mode decision
- POST route admission
- `/api/state.control_plane_security` export

When `write_api_available=false`, POST writes return `503`.

## Token Policy

`POLY_CONTROL_TOKEN` must be strong:

- missing/empty -> invalid
- length `< CONTROL_TOKEN_MIN_LENGTH` -> invalid
- weak dictionary tokens (`token`, `password`, `changeme`, etc.) -> invalid

In live mode (`DRY_RUN=false`), if `POLY_ENABLE_WRITE_API=true` but token invalid, startup exits fail-closed.

## Source Policy and Trusted Proxy

`POLY_CONTROL_SOURCE_POLICY`:

- `local_only` (default): loopback only
- `internal_only`: loopback + private RFC1918
- `any`: allow all

`X-Forwarded-For` is ignored by default.
Only when both are true will XFF be parsed:

1. `POLY_TRUSTED_PROXY_CIDRS` is configured
2. socket `remote_addr` belongs to trusted proxy CIDR set

Otherwise source identity uses socket remote address only.

## Auditing

Every write request emits audit events to `POLY_CONTROL_AUDIT_LOG_PATH` (default runtime path `control_audit_events.jsonl`):

- rejected writes (missing/invalid token, source blocked, write disabled, route blocked)
- accepted + successful writes

Audit fields include:

- `ts`, `method`, `path`, `action`, `status`, `reason_code`, `http_status`
- `source_ip`, `client_ip`, `xff_used`
- `write_api_available`, `live_mode`, `authorized`
- `writer_scope`, `token_configured`, `source_policy`

No raw token is exported in audit records.

## `/api/state` Operator Security Fields

`/api/state` includes `control_plane_security`:

- `token_configured`
- `write_api_requested`
- `write_api_available`
- `write_api_enabled`
- `readonly_mode`
- `live_mode`
- `source_policy`
- `trusted_proxy_configured`
- `reason_codes`

## Decision Table

- `write_api_requested=false` -> `write_api_available=false`, `readonly_mode=true`, reads allowed, writes rejected (`503`).
- `write_api_requested=true` + token missing/weak -> write disabled (`503` on POST), and in live mode startup fails (`SystemExit 4`).
- `write_api_requested=true` + token valid + writer lock conflict -> write disabled (`503`, `reason_code=single_writer_conflict`).
- `write_api_requested=true` + token valid + source blocked -> POST rejected (`403`, `reason_code=source_not_allowed`).
- `write_api_requested=true` + token invalid at request time -> POST rejected (`401`, `reason_code=control_token_missing|control_token_invalid`).
- `write_api_requested=true` + token/source/lock all valid -> write request admitted and audited; state mutation proceeds.

## Verification

Run:

```bash
PYTHONPATH=src .venv/bin/python scripts/verify_control_auth.py
PYTHONPATH=src .venv/bin/python scripts/verify_write_api_local_only.py
bash scripts/gates/gate_block_item.sh BLOCK-006
```
