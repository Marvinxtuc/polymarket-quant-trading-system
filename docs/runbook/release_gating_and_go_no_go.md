# Release Gating and Go/No-Go Runbook (BLOCK-010)

## Scope

BLOCK-010 defines the final pre-release gate.

- It does **not** replace BLOCK-001~009 logic.
- It aggregates their gate outcomes into one release verdict.
- Failures default to **NO-GO**.

## Single Source of Required Blocks

Required blocks are defined in exactly one place:

- `scripts/gates/release_blocks.json`

All of the following read the same file:

- `scripts/gates/gate_release_readiness.sh`
- `scripts/verify_release_readiness.py`
- release gate unit/integration tests
- JSON/Markdown release reports

## Release Gate Inputs

The final decision uses only:

1. `gate_block_item.sh <BLOCK-ID>` exit code
2. standard machine result line:
   - `GATE_BLOCK_RESULT block_id=... static=... tests=... behavior=... docs=... overall=...`
3. required block evidence files:
   - `reports/blocking/<BLOCK-ID>/validation.txt`
   - `reports/blocking/<BLOCK-ID>/regression.txt`
   - `reports/blocking/<BLOCK-ID>/self_check.md`
4. execution metadata:
   - git branch
   - git commit
   - UTC timestamp
   - release gate command

Free-text logs are never used as the only pass/fail signal.

## Decision Model

Required blocks (default): `BLOCK-001` ~ `BLOCK-009`.

### GO conditions

All required blocks satisfy all checks:

- gate exit code is `0`
- machine result line exists and parses successfully
- machine result `overall=0`
- all three block evidence files exist, are non-empty, and pass minimum structure checks

### NO-GO conditions

Any of the following causes NO-GO:

- any required block gate exit code non-zero
- missing or invalid machine result line
- machine result block mismatch
- machine result `overall!=0`
- missing/empty/invalid required report files
- release gate internal error (parse/write/runtime)

There is no default "ignore failures and continue GO" mode.

## Report Outputs

Release gate writes two atomic outputs:

- `reports/release/go_no_go_summary.json`
- `reports/release/go_no_go_summary.md`

Both include:

- final decision (`GO` / `NO-GO`)
- required block list
- per-block status and failure reasons
- execution timestamp
- git branch + git commit
- release gate command

Atomic write rule:

- write to temporary file
- replace destination with `os.replace`
- partial files are not treated as valid output

## Operator Command

Run final release gate:

```bash
bash scripts/gates/gate_release_readiness.sh
```

Exit code semantics:

- `0` -> GO
- non-zero -> NO-GO
