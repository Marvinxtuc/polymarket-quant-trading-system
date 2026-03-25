from __future__ import annotations

import time
import tempfile
import unittest
from pathlib import Path
from datetime import datetime, timezone

from polymarket_bot.db import PersonalTerminalStore
from polymarket_bot.types import Candidate, JournalEntry, WalletProfile


class PersonalTerminalStoreTests(unittest.TestCase):
    def test_connection_enables_wal_and_busy_timeout(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store = PersonalTerminalStore(str(Path(tmpdir) / "terminal.db"))
            with store.connection() as conn:
                journal_mode = str(conn.execute("PRAGMA journal_mode").fetchone()[0]).lower()
                busy_timeout = int(conn.execute("PRAGMA busy_timeout").fetchone()[0])

            self.assertEqual(journal_mode, "wal")
            self.assertEqual(busy_timeout, 5000)

    def test_close_marks_store_unusable_until_reentered(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            store = PersonalTerminalStore(str(Path(tmpdir) / "terminal.db"))
            store.close()
            with self.assertRaises(RuntimeError):
                store.list_candidates()
            with store:
                self.assertEqual(store.list_candidates(), [])

    def test_candidates_roundtrip_and_action_updates_status(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with PersonalTerminalStore(str(Path(tmpdir) / "terminal.db")) as store:
                now = int(time.time())

                store.upsert_candidate(
                    Candidate(
                        id="cand-a",
                        signal_id="sig-a",
                        trace_id="trc-a",
                        wallet="0xa",
                        market_slug="market-a",
                        token_id="token-a",
                        outcome="YES",
                        side="BUY",
                        confidence=0.9,
                        wallet_score=82.0,
                        score=84.5,
                        suggested_action="follow",
                        created_ts=now,
                        expires_ts=now + 3600,
                        updated_ts=now,
                        signal_snapshot={"signal_id": "sig-a"},
                    )
                )
                store.upsert_candidate(
                    Candidate(
                        id="cand-b",
                        signal_id="sig-b",
                        trace_id="trc-b",
                        wallet="0xb",
                        market_slug="market-b",
                        token_id="token-b",
                        outcome="NO",
                        side="BUY",
                        confidence=0.7,
                        wallet_score=64.0,
                        score=66.0,
                        suggested_action="buy_small",
                        status="watched",
                        created_ts=now + 10,
                        expires_ts=now + 3600,
                        updated_ts=now + 10,
                        signal_snapshot={"signal_id": "sig-b"},
                    )
                )

                first = store.record_candidate_action(
                    "cand-a",
                    action="follow",
                    note="looks good",
                    created_ts=now + 20,
                    idempotency_key="cand-a-follow",
                )
                second = store.record_candidate_action(
                    "cand-a",
                    action="follow",
                    note="looks good",
                    created_ts=now + 21,
                    idempotency_key="cand-a-follow",
                )
                approved = store.list_candidates(statuses=["approved"])
                pending_actions = store.list_pending_actions()
                candidate = store.get_candidate("cand-a")

                self.assertEqual(len(approved), 1)
                self.assertEqual(approved[0]["id"], "cand-a")
                self.assertEqual(candidate["status"], "approved")
                self.assertEqual(candidate["selected_action"], "follow")
                self.assertEqual(len(pending_actions), 2)
                self.assertEqual([row["id"] for row in store.list_candidates(limit=4)], ["cand-a", "cand-b"])
                self.assertFalse(first["_idempotent_replay"])
                self.assertTrue(second["_idempotent_replay"])
                self.assertEqual(
                    [row["id"] for row in store.list_candidates(limit=4, wallet="0xa", market_slug="market-a")],
                    ["cand-a"],
                )

    def test_candidates_support_search_sort_lookup_and_detail_chain(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with PersonalTerminalStore(str(Path(tmpdir) / "terminal.db")) as store:
                now = int(time.time())

                store.upsert_candidate(
                    Candidate(
                        id="cand-low",
                        signal_id="sig-low",
                        trace_id="trc-low",
                        wallet="0xbbb",
                        market_slug="beta-market",
                        token_id="token-b",
                        outcome="NO",
                        side="SELL",
                        confidence=0.6,
                        score=41.0,
                        suggested_action="watch",
                        status="watched",
                        created_ts=now - 10,
                        expires_ts=now + 3600,
                        updated_ts=now - 10,
                        signal_snapshot={"signal_id": "sig-low", "trace_id": "trc-low", "note": "beta watch"},
                    )
                )
                store.upsert_candidate(
                    Candidate(
                        id="cand-high",
                        signal_id="sig-high",
                        trace_id="trc-high",
                        wallet="0xaaa",
                        market_slug="alpha-market",
                        token_id="token-a",
                        outcome="YES",
                        side="BUY",
                        confidence=0.91,
                        score=88.0,
                        suggested_action="follow",
                        created_ts=now,
                        expires_ts=now + 3600,
                        updated_ts=now,
                        signal_snapshot={"signal_id": "sig-high", "trace_id": "trc-high", "note": "alpha follow"},
                    )
                )
                store.record_candidate_action(
                    "cand-high",
                    action="follow",
                    note="looks strong",
                    created_ts=now + 20,
                    idempotency_key="cand-high-follow",
                )
                store.record_candidate_action(
                    "cand-low",
                    action="watch",
                    note="needs confirmation",
                    created_ts=now + 30,
                    idempotency_key="cand-low-watch",
                )
                store.append_journal_entry(
                    JournalEntry(
                        candidate_id="cand-high",
                        action="follow",
                        rationale="looks strong",
                        result_tag="filled",
                        created_ts=now + 40,
                        market_slug="alpha-market",
                        wallet="0xaaa",
                    )
                )
                store.append_journal_entry(
                    JournalEntry(
                        candidate_id="cand-low",
                        action="watch",
                        rationale="needs confirmation",
                        result_tag="observed",
                        created_ts=now + 50,
                        market_slug="beta-market",
                        wallet="0xbbb",
                    )
                )

                self.assertEqual([row["id"] for row in store.list_candidates(sort="score", order="desc", limit=4)], ["cand-high", "cand-low"])
                self.assertEqual([row["id"] for row in store.list_candidates(sort="score", order="asc", limit=4)], ["cand-low", "cand-high"])
                self.assertEqual([row["id"] for row in store.list_candidates(search="alpha", limit=4)], ["cand-high"])
                self.assertEqual([row["id"] for row in store.list_candidates(search="beta watch", limit=4)], ["cand-low"])
                self.assertEqual([row["id"] for row in store.list_candidates(action="follow", limit=4)], ["cand-high"])
                self.assertEqual([row["id"] for row in store.list_candidates(candidate_id="cand-high", limit=4)], ["cand-high"])

                by_signal = store.find_candidate("sig-high")
                by_trace = store.find_candidate("trc-high")
                detail = store.candidate_detail("trc-high", related_limit=10)

                self.assertIsNotNone(by_signal)
                self.assertEqual(by_signal["id"], "cand-high")
                self.assertIsNotNone(by_trace)
                self.assertEqual(by_trace["id"], "cand-high")
                self.assertIsNotNone(detail)
                self.assertEqual(detail["candidate"]["id"], "cand-high")
                self.assertEqual(detail["candidate_id"], "cand-high")
                self.assertEqual(detail["signal_id"], "sig-high")
                self.assertEqual(detail["trace_id"], "trc-high")
                self.assertEqual(detail["summary"]["related_action_count"], 1)
                self.assertEqual(detail["summary"]["related_journal_count"], 1)
                self.assertEqual(detail["related_actions"][0]["action"], "follow")
                self.assertEqual(detail["related_journal"][0]["action"], "follow")

    def test_upsert_candidate_folds_active_duplicate_pending_rows(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with PersonalTerminalStore(str(Path(tmpdir) / "terminal.db")) as store:
                now = int(time.time())

                store.upsert_candidate(
                    Candidate(
                        id="cand-first",
                        signal_id="sig-first",
                        trace_id="trc-first",
                        wallet="0xdup",
                        market_slug="dup-market",
                        token_id="dup-token",
                        outcome="YES",
                        side="BUY",
                        confidence=0.8,
                        score=61.0,
                        suggested_action="buy_small",
                        status="pending",
                        created_ts=now,
                        expires_ts=now + 300,
                        updated_ts=now,
                    )
                )
                store.upsert_candidate(
                    Candidate(
                        id="cand-second",
                        signal_id="sig-second",
                        trace_id="trc-second",
                        wallet="0xdup",
                        market_slug="dup-market",
                        token_id="dup-token",
                        outcome="YES",
                        side="BUY",
                        confidence=0.82,
                        score=64.0,
                        suggested_action="buy_small",
                        status="pending",
                        created_ts=now + 10,
                        expires_ts=now + 900,
                        updated_ts=now + 10,
                    )
                )

                pending = store.list_candidates(statuses=["pending"], include_expired=True, limit=10)

                self.assertEqual(len(pending), 1)
                self.assertEqual(pending[0]["id"], "cand-first")
                self.assertEqual(pending[0]["signal_id"], "sig-second")
                self.assertEqual(pending[0]["trace_id"], "trc-second")
                self.assertEqual(int(pending[0]["created_ts"] or 0), now)
                self.assertEqual(int(pending[0]["expires_ts"] or 0), now + 900)

    def test_candidates_expire_when_short_market_window_has_ended(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with PersonalTerminalStore(str(Path(tmpdir) / "terminal.db")) as store:
                now = int(time.time())
                market_start = now - 600
                market_end = market_start + 300

                store.upsert_candidate(
                    Candidate(
                        id="cand-expired-window",
                        signal_id="sig-expired-window",
                        trace_id="trc-expired-window",
                        wallet="0xstale",
                        market_slug=f"btc-updown-5m-{market_start}",
                        token_id="token-expired-window",
                        outcome="YES",
                        side="BUY",
                        confidence=0.82,
                        score=71.0,
                        suggested_action="watch",
                        created_ts=now - 120,
                        expires_ts=now + 3600,
                        updated_ts=now - 120,
                    )
                )

                self.assertEqual(store.list_candidates(limit=4), [])
                expired = store.get_candidate("cand-expired-window")

                self.assertIsNotNone(expired)
                self.assertEqual(expired["status"], "expired")
                self.assertEqual(int(expired["expires_ts"] or 0), market_end)
                self.assertEqual([row["id"] for row in store.list_candidates(limit=4, include_expired=True)], ["cand-expired-window"])

    def test_list_candidates_hides_expired_rows_by_default(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with PersonalTerminalStore(str(Path(tmpdir) / "terminal.db")) as store:
                now = int(time.time())

                store.upsert_candidate(
                    Candidate(
                        id="cand-expired-visible",
                        signal_id="sig-expired-visible",
                        trace_id="trc-expired-visible",
                        wallet="0xstale",
                        market_slug="btc-updown-15m-1773913500",
                        token_id="token-expired-visible",
                        outcome="YES",
                        side="BUY",
                        confidence=0.82,
                        score=71.0,
                        suggested_action="watch",
                        status="expired",
                        created_ts=now - 120,
                        expires_ts=now + 3600,
                        updated_ts=now - 60,
                    )
                )

                self.assertEqual(store.list_candidates(limit=4), [])
                self.assertEqual(
                    [row["id"] for row in store.list_candidates(limit=4, include_expired=True)],
                    ["cand-expired-visible"],
                )

    def test_wallet_profiles_and_journal_summary(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with PersonalTerminalStore(str(Path(tmpdir) / "terminal.db")) as store:
                now = int(time.time())

                store.upsert_wallet_profile(
                    WalletProfile(
                        wallet="0xaaa",
                        tag="CORE",
                        trust_score=88.0,
                        followability_score=91.0,
                        category="politics",
                        enabled=True,
                        notes="seed wallet",
                        updated_ts=now,
                    )
                )
                store.upsert_wallet_profile(
                    WalletProfile(
                        wallet="0xbbb",
                        tag="WATCH",
                        trust_score=55.0,
                        followability_score=50.0,
                        category="sports",
                        enabled=False,
                        notes="too noisy",
                        updated_ts=now - 10,
                    )
                )
                store.append_journal_entry(
                    JournalEntry(
                        candidate_id="cand-a",
                        action="follow",
                        rationale="spread ok",
                        result_tag="filled",
                        created_ts=now,
                        market_slug="market-a",
                        wallet="0xaaa",
                    )
                )
                store.append_journal_entry(
                    JournalEntry(
                        candidate_id="cand-b",
                        action="watch",
                        rationale="want confirmation",
                        result_tag=None,
                        created_ts=now + 10,
                        market_slug="market-b",
                        wallet="0xbbb",
                    )
                )

                profiles = store.list_wallet_profiles(limit=4)
                journal = store.list_journal_entries(limit=4)
                summary = store.journal_summary(days=30)

                self.assertEqual([row["wallet"] for row in profiles], ["0xaaa", "0xbbb"])
                self.assertEqual(journal[0]["candidate_id"], "cand-b")
                self.assertEqual(journal[1]["candidate_id"], "cand-a")
                self.assertEqual(summary["total_entries"], 2)
                self.assertEqual(summary["execution_actions"], 1)
                self.assertEqual(summary["watch_actions"], 1)
                self.assertEqual(summary["ignore_actions"], 0)

    def test_stats_and_archive_rollups_cover_candidates_actions_and_journal(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            with PersonalTerminalStore(str(Path(tmpdir) / "terminal.db")) as store:
                now = int(time.time())
                day2 = now - 12 * 3600
                day1 = day2 - 2 * 86400

                store.upsert_candidate(
                    Candidate(
                        id="cand-a",
                        signal_id="sig-a",
                        trace_id="trc-a",
                        wallet="0xa",
                        market_slug="market-a",
                        token_id="token-a",
                        outcome="YES",
                        side="BUY",
                        confidence=0.9,
                        score=81.0,
                        created_ts=day1,
                        expires_ts=day1 + 86400,
                        updated_ts=day1,
                        signal_snapshot={"signal_id": "sig-a"},
                    )
                )
                store.upsert_candidate(
                    Candidate(
                        id="cand-b",
                        signal_id="sig-b",
                        trace_id="trc-b",
                        wallet="0xb",
                        market_slug="market-b",
                        token_id="token-b",
                        outcome="NO",
                        side="BUY",
                        confidence=0.7,
                        score=65.0,
                        created_ts=day2,
                        expires_ts=day2 + 86400,
                        updated_ts=day2,
                        signal_snapshot={"signal_id": "sig-b"},
                    )
                )
                store.record_candidate_action(
                    "cand-a",
                    action="watch",
                    note="monitor first",
                    created_ts=day1 + 120,
                    idempotency_key="cand-a-watch",
                )
                store.record_candidate_action(
                    "cand-b",
                    action="follow",
                    note="size it",
                    created_ts=day2 + 120,
                    idempotency_key="cand-b-follow",
                )
                store.append_journal_entry(
                    JournalEntry(
                        candidate_id="cand-a",
                        action="watch",
                        rationale="monitor first",
                        result_tag="observed",
                        created_ts=day1 + 240,
                        market_slug="market-a",
                        wallet="0xa",
                    )
                )
                store.append_journal_entry(
                    JournalEntry(
                        candidate_id="cand-b",
                        action="follow",
                        rationale="size it",
                        result_tag="filled",
                        pnl_realized=1.5,
                        created_ts=day2 + 240,
                        market_slug="market-b",
                        wallet="0xb",
                    )
                )

                candidate_summary = store.candidate_summary(days=30)
                action_summary = store.candidate_action_summary(days=30)
                archive = store.archive_summary(days=30, recent_days=1)
                stats = store.stats_summary(days=30, recent_days=1)
                actions = store.list_candidate_actions(limit=10, days=30)

                self.assertEqual(candidate_summary["total_candidates"], 2)
                self.assertEqual({row["status"]: row["count"] for row in candidate_summary["by_status"]}, {"approved": 1, "watched": 1})
                self.assertEqual(action_summary["total_actions"], 2)
                self.assertEqual(len(actions), 2)
                self.assertEqual(archive["day_count"], 2)
                self.assertEqual(archive["summary"]["candidate_count"], 2)
                self.assertEqual(archive["summary"]["action_count"], 2)
                self.assertEqual(archive["summary"]["journal_count"], 2)
                self.assertEqual(archive["recent_summary"]["candidate_count"], 1)
                self.assertEqual(stats["totals"]["candidate_count"], 2)
                self.assertEqual(stats["totals"]["action_count"], 2)
                self.assertEqual(stats["totals"]["journal_count"], 2)


if __name__ == "__main__":
    unittest.main()
