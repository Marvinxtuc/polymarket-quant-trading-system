from __future__ import annotations

import tempfile
import time
import unittest
from pathlib import Path

from polymarket_bot.db import PersonalTerminalStore
from polymarket_bot.types import Candidate


class CandidateLifetimeExpirationTests(unittest.TestCase):
    def test_expire_candidates_sets_reason_layer_and_lifecycle_state(self):
        db_path = str(Path(tempfile.mkdtemp()) / "terminal.db")
        store = PersonalTerminalStore(db_path)
        self.addCleanup(store.close)

        now_ts = int(time.time())
        store.upsert_candidate(
            Candidate(
                id="cand-expire-now",
                signal_id="sig-expire-now",
                trace_id="trc-expire-now",
                wallet="0xabc",
                market_slug="demo-market",
                token_id="token-expire-now",
                outcome="YES",
                side="BUY",
                confidence=0.8,
                score=80.0,
                status="approved",
                created_ts=now_ts - 1800,
                expires_ts=now_ts - 1,
                updated_ts=now_ts - 1800,
                signal_snapshot={"signal_id": "sig-expire-now", "timestamp": "2026-03-30T00:00:00+00:00"},
            )
        )

        updated = store.expire_candidates(now=now_ts)
        candidate = store.get_candidate("cand-expire-now")

        self.assertEqual(updated, 1)
        self.assertIsNotNone(candidate)
        assert candidate is not None
        self.assertEqual(candidate["status"], "expired")
        self.assertEqual(candidate["block_reason"], "candidate_lifetime_expired")
        self.assertEqual(candidate["block_layer"], "candidate")
        self.assertEqual(candidate["lifecycle_state"], "expired_discarded")

    def test_candidate_lifecycle_summary_tracks_reason_layer_counts(self):
        db_path = str(Path(tempfile.mkdtemp()) / "terminal.db")
        store = PersonalTerminalStore(db_path)
        self.addCleanup(store.close)

        now_ts = int(time.time())
        store.upsert_candidate(
            Candidate(
                id="cand-expired-summary",
                signal_id="sig-expired-summary",
                trace_id="trc-expired-summary",
                wallet="0xabc",
                market_slug="demo-market",
                token_id="token-expired-summary",
                outcome="YES",
                side="BUY",
                confidence=0.8,
                score=80.0,
                status="expired",
                lifecycle_state="expired_discarded",
                block_reason="candidate_lifetime_expired",
                block_layer="execution_precheck",
                created_ts=now_ts - 2000,
                expires_ts=now_ts - 1000,
                updated_ts=now_ts - 10,
                signal_snapshot={"signal_id": "sig-expired-summary"},
            )
        )

        summary = store.candidate_lifecycle_summary(limit=10)

        self.assertEqual(summary["expired_discarded_count"], 1)
        self.assertEqual(summary["block_reasons"]["candidate_lifetime_expired"], 1)
        self.assertEqual(summary["block_layers"]["execution_precheck"], 1)
        self.assertEqual(summary["reason_layer_counts"]["candidate_lifetime_expired"]["execution_precheck"], 1)


if __name__ == "__main__":
    unittest.main()
