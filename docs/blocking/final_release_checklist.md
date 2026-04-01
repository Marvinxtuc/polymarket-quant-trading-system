# Final Release Checklist (BLOCK-010)

## Required Blocks

Source of truth: `scripts/gates/release_blocks.json`

Current required blocks:

- BLOCK-001
- BLOCK-002
- BLOCK-003
- BLOCK-004
- BLOCK-005
- BLOCK-006
- BLOCK-007
- BLOCK-008
- BLOCK-009

## Execution Steps

1. Run final release gate:
   - `bash scripts/gates/gate_release_readiness.sh`
2. Confirm exit code:
   - `0` => GO
   - non-zero => NO-GO
3. Confirm release artifacts exist:
   - `reports/release/go_no_go_summary.json`
   - `reports/release/go_no_go_summary.md`

## Required Report Fields

Release summary must include:

- `go_no_go`
- `execution_timestamp_utc`
- `git_branch`
- `git_commit`
- `release_gate_command`
- `required_blocks`
- per-block pass/fail and failure reasons

## Block Evidence Requirements

For each required block:

- `reports/blocking/<BLOCK-ID>/validation.txt`
- `reports/blocking/<BLOCK-ID>/regression.txt`
- `reports/blocking/<BLOCK-ID>/self_check.md`

Gate rejects GO if any required evidence file is missing, empty, or invalid.
