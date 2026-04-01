#!/usr/bin/env python3
from __future__ import annotations

import argparse
import importlib.util
import json
import sys
import time
import unittest
from dataclasses import dataclass
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from polymarket_bot.i18n import t as i18n_t


def _drill_t(key: str, params: dict[str, object] | None = None, *, fallback: str = "") -> str:
    return i18n_t(f"report.faultDrill.{key}", dict(params or {}), fallback=fallback)


@dataclass(frozen=True)
class DrillTest:
    file_path: str
    class_name: str
    test_name: str
    purpose_key: str

    @property
    def label(self) -> str:
        return f"{self.file_path}::{self.class_name}.{self.test_name}"


DRILLS: dict[str, dict[str, object]] = {
    "startup_gate": {
        "title_key": "category.startupGate.title",
        "goal_key": "category.startupGate.goal",
        "tests": [
            DrillTest(
                "tests/test_runner_control.py",
                "RiskManagerTests",
                "test_trading_mode_blocks_buy_but_not_sell",
                "testPurpose.systemTradingModeBlocksBuy",
            ),
            DrillTest(
                "tests/test_runner_control.py",
                "TraderControlTests",
                "test_account_state_stale_blocks_new_buy_but_allows_sell",
                "testPurpose.accountStateStaleBlocksBuy",
            ),
            DrillTest(
                "tests/test_runner_control.py",
                "TraderControlTests",
                "test_reconciliation_warn_blocks_new_buy",
                "testPurpose.reconciliationWarnBlocksBuy",
            ),
        ],
    },
    "persistence_fault": {
        "title_key": "category.persistenceFault.title",
        "goal_key": "category.persistenceFault.goal",
        "tests": [
            DrillTest(
                "tests/test_runner_control.py",
                "TraderControlTests",
                "test_ledger_append_failure_marks_persistence_fault_and_halts_opening",
                "testPurpose.ledgerAppendFailureHaltsOpening",
            ),
            DrillTest(
                "tests/test_runner_control.py",
                "TraderControlTests",
                "test_runtime_state_persist_failure_marks_persistence_fault_and_raises",
                "testPurpose.runtimeStatePersistFailureHaltsOpening",
            ),
            DrillTest(
                "tests/test_runner_control.py",
                "TraderControlTests",
                "test_persistence_fault_cancels_pending_buy_orders",
                "testPurpose.persistenceFaultCancelsPendingBuyOrders",
            ),
            DrillTest(
                "tests/test_daemon_state.py",
                "DaemonStateTests",
                "test_persist_cycle_outputs_records_state_write_fault_and_raises",
                "testPurpose.daemonStateWriteFaultSurfaced",
            ),
        ],
    },
    "reconcile_ambiguity": {
        "title_key": "category.reconcileAmbiguity.title",
        "goal_key": "category.reconcileAmbiguity.goal",
        "tests": [
            DrillTest(
                "tests/test_runner_control.py",
                "TraderControlTests",
                "test_multiple_pending_same_token_without_order_fill_marks_ambiguous",
                "testPurpose.multiplePendingOrdersMarkedAmbiguous",
            ),
        ],
    },
}


def _load_module(path: Path, cache: dict[Path, object]) -> object:
    cached = cache.get(path)
    if cached is not None:
        return cached
    module_name = f"fault_drill_{path.stem}"
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"unable to load module spec for {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    cache[path] = module
    return module


def _build_suite(selected_categories: list[str]) -> tuple[unittest.TestSuite, list[dict[str, object]]]:
    suite = unittest.TestSuite()
    cache: dict[Path, object] = {}
    manifest: list[dict[str, object]] = []
    for category in selected_categories:
        category_config = DRILLS[category]
        tests = list(category_config["tests"])
        manifest.append(
            {
                "category": category,
                "title": _drill_t(str(category_config["title_key"]), fallback=str(category_config["title_key"])),
                "goal": _drill_t(str(category_config["goal_key"]), fallback=str(category_config["goal_key"])),
                "tests": [
                    {
                        "label": test.label,
                        "purpose": _drill_t(test.purpose_key, fallback=test.purpose_key),
                    }
                    for test in tests
                ],
            }
        )
        for test in tests:
            path = ROOT / test.file_path
            module = _load_module(path, cache)
            cls = getattr(module, test.class_name)
            suite.addTest(cls(test.test_name))
    return suite, manifest


def _result_summary(result: unittest.TestResult, *, duration_seconds: float, categories: list[str], manifest: list[dict[str, object]]) -> dict[str, object]:
    failures = [
        {
            "test": case.id(),
            "traceback": trace,
        }
        for case, trace in list(result.failures) + list(result.errors)
    ]
    return {
        "ok": bool(result.wasSuccessful()),
        "categories": categories,
        "tests_run": int(result.testsRun),
        "failures": len(result.failures),
        "errors": len(result.errors),
        "skipped": len(result.skipped),
        "duration_seconds": round(duration_seconds, 3),
        "manifest": manifest,
        "failed_tests": failures,
    }


def _print_text(summary: dict[str, object]) -> None:
    print(_drill_t("title", fallback="Polymarket Fault Drill"))
    print(_drill_t("field.ok", {"value": summary["ok"]}, fallback=f"ok: {summary['ok']}"))
    print(_drill_t("field.testsRun", {"value": summary["tests_run"]}, fallback=f"tests_run: {summary['tests_run']}"))
    print(
        _drill_t(
            "field.durationSeconds",
            {"value": summary["duration_seconds"]},
            fallback=f"duration_seconds: {summary['duration_seconds']}",
        )
    )
    print("")
    for category in list(summary["manifest"]):
        print(
            _drill_t(
                "row.category",
                {"category": category["category"], "title": category["title"]},
                fallback=f"[DRILL] {category['category']}: {category['title']}",
            )
        )
        print(_drill_t("field.goal", {"value": category["goal"]}, fallback=f"  goal: {category['goal']}"))
        for item in list(category["tests"]):
            print(_drill_t("row.testLabel", {"value": item["label"]}, fallback=f"  - {item['label']}"))
            print(_drill_t("row.testPurpose", {"value": item["purpose"]}, fallback=f"    purpose: {item['purpose']}"))
        print("")
    if summary["ok"]:
        print(_drill_t("field.resultPass", fallback="result: PASS"))
        print(
            _drill_t(
                "field.coverage",
                fallback="coverage: startup gate, persistence halt, and reconcile ambiguity protections are all exercised.",
            )
        )
        return
    print(_drill_t("field.resultFail", fallback="result: FAIL"))
    for failed in list(summary["failed_tests"]):
        print(_drill_t("row.testLabel", {"value": failed["test"]}, fallback=f"  - {failed['test']}"))
        print(_drill_t("row.traceback", {"value": failed["traceback"].strip()}, fallback=f"    {failed['traceback'].strip()}"))


def main() -> int:
    parser = argparse.ArgumentParser(
        description=_drill_t(
            "cli.description",
            fallback="Run curated control-plane fault drills for the Polymarket stack",
        )
    )
    parser.add_argument(
        "--category",
        action="append",
        choices=sorted(DRILLS.keys()),
        help=_drill_t(
            "cli.category",
            fallback="Limit execution to one or more drill categories. Defaults to all.",
        ),
    )
    parser.add_argument("--json", action="store_true", help=_drill_t("cli.json", fallback="Emit JSON instead of text"))
    parser.add_argument(
        "--verbose",
        action="store_true",
        help=_drill_t("cli.verbose", fallback="Use verbose unittest output"),
    )
    args = parser.parse_args()

    categories = list(args.category or DRILLS.keys())
    suite, manifest = _build_suite(categories)
    verbosity = 2 if args.verbose else 1
    start = time.time()
    result = unittest.TextTestRunner(stream=sys.stderr, verbosity=verbosity).run(suite)
    duration_seconds = time.time() - start
    summary = _result_summary(
        result,
        duration_seconds=duration_seconds,
        categories=categories,
        manifest=manifest,
    )
    if args.json:
        print(json.dumps(summary, ensure_ascii=False, indent=2))
    else:
        _print_text(summary)
    return 0 if summary["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
