# Demo Core Freeze v1

## Scope

This freeze covers the Phase 1 demo core for the Polymarket trading system:

- `demo_loop`
- `demo_risk`
- `demo_ledger`
- CLI entry wiring in `main.py`

This freeze does not include:

- real trading execution
- live broker paths
- fees
- short inventory accounting
- frontend/demo state integration work

## Ledger Truth Layer

The ledger truth layer for Phase 1 is:

- `orders.jsonl`
- `fills.jsonl`
- `positions.json`
- `equity.jsonl`

Audit and pass/fail decisions must be based on these files.

## Projection Layer

The projection layer is:

- `state.json`

`state.json` is a display/projection artifact only. It is not a source of truth for ledger audit decisions.

## Frozen Code Boundary

The Phase 1 freeze code boundary is:

- `src/polymarket_bot/main.py`
- `src/polymarket_bot/demo_loop.py`
- `src/polymarket_bot/demo_risk.py`
- `src/polymarket_bot/demo_ledger.py`

## Excluded From Freeze

The following are explicitly excluded from the formal freeze package:

- `runtime/`
- single-run demo outputs
- single-run risk outputs
- single-run ledger outputs
- local evidence copies
- temporary debug artifacts

Repository strategy:

- `runtime/` must remain excluded by repository ignore rules
- runtime evidence may be kept locally for audit, but must not be mixed into the code freeze commit

## Baseline Commands

All baseline commands are locked to the project virtual environment.

### Demo loop baseline

```bash
cd /Users/marvin.x/Desktop/Polymarket
PYTHONPATH=src ./.venv/bin/python -m polymarket_bot.main --demo-mode --demo-seed 42 --demo-max-ticks 12 --demo-tick-seconds 1
```

### Risk suite baseline

```bash
cd /Users/marvin.x/Desktop/Polymarket
PYTHONPATH=src ./.venv/bin/python -m polymarket_bot.main --demo-risk-suite --demo-seed 42
```

### Ledger suite baseline

```bash
cd /Users/marvin.x/Desktop/Polymarket
PYTHONPATH=src ./.venv/bin/python -m polymarket_bot.main --demo-ledger-suite
```

### PM-FLOW-005A regression baseline

```bash
cd /Users/marvin.x/Desktop/Polymarket
PYTHONPATH=src ./.venv/bin/python - <<'PY'
from polymarket_bot.demo_ledger import run_demo_ledger_suite
import json

result = run_demo_ledger_suite(
    scenario_ids=[
        "invalid_time_order_failed_hard",
        "single_full_fill_open",
        "partial_then_full",
        "reject_pollution_free",
    ]
)
print(json.dumps(result, ensure_ascii=False, indent=2))
PY
```

## Time-Semantic Guard

Phase 1 requires pre-apply validation for invalid fill timing:

- invalid fill must be blocked before truth apply
- invalid fill must not enter `fills.jsonl`
- invalid fill must not mutate `positions.json`
- invalid fill must not append `equity.jsonl`
- invalid fill must not change cash

The execution interpretation of `truth layer unchanged` is:

- `truth layer has no incremental mutation caused by invalid fill`

## Open Items / Phase 2 Entry

Not included in this freeze:

- frontend consumption of demo/risk state
- automated assertions around fixed seeds in CI
- concurrent/repeated-call reliability verification
- breaker lifecycle verification
- fees
- short inventory accounting
