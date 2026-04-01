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

from test_candidate_lifetime_expiration import CandidateLifetimeExpirationTests  # noqa: E402
from test_daemon_state import DaemonStateTests  # noqa: E402
from test_runner_control import TraderControlTests  # noqa: E402
from test_web_api import WebApiTests  # noqa: E402


def main() -> int:
    suite = unittest.TestSuite()
    suite.addTest(CandidateLifetimeExpirationTests("test_expire_candidates_sets_reason_layer_and_lifecycle_state"))
    suite.addTest(CandidateLifetimeExpirationTests("test_candidate_lifecycle_summary_tracks_reason_layer_counts"))
    suite.addTest(TraderControlTests("test_candidate_lifetime_uses_generation_timestamp_not_signal_timestamp"))
    suite.addTest(TraderControlTests("test_approved_candidate_expires_at_decision_layer_before_queue"))
    suite.addTest(TraderControlTests("test_stale_queued_candidate_cannot_bypass_execution_precheck"))
    suite.addTest(DaemonStateTests("test_build_state_surfaces_recent_cycle_skip_observability_without_active_candidates"))
    suite.addTest(WebApiTests("test_state_and_metrics_surface_candidate_lifetime_expiration_summary"))

    result = unittest.TextTestRunner(verbosity=2).run(suite)
    if not result.wasSuccessful():
        return 1
    print("verify_candidate_lifetime_expiration: ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
