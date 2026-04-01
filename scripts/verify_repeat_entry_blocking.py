#!/usr/bin/env python3
from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
TESTS = ROOT / "tests"

for candidate in (SRC, TESTS):
    if str(candidate) not in sys.path:
        sys.path.insert(0, str(candidate))

from test_daemon_state import DaemonStateTests  # noqa: E402
from test_runner_control import TraderControlTests  # noqa: E402
from test_web_api import WebApiTests  # noqa: E402


def main() -> int:
    suite = unittest.TestSuite()
    suite.addTest(TraderControlTests("test_candidate_blocks_same_wallet_add_by_default"))
    suite.addTest(TraderControlTests("test_same_wallet_add_requires_explicit_allowlist"))
    suite.addTest(TraderControlTests("test_cross_wallet_repeat_entry_does_not_enlarge_existing_position_in_auto_mode"))
    suite.addTest(TraderControlTests("test_manual_approved_repeat_entry_cannot_bypass_execution_precheck"))
    suite.addTest(DaemonStateTests("test_build_state_surfaces_recent_cycle_skip_observability_without_active_candidates"))
    suite.addTest(WebApiTests("test_candidates_endpoint_falls_back_to_signal_review_when_store_and_runtime_queue_empty"))

    result = unittest.TextTestRunner(verbosity=2).run(suite)
    if not result.wasSuccessful():
        return 1
    print("verify_repeat_entry_blocking: ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
