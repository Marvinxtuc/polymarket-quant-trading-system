from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from polymarket_bot.locks import (
    FileLock,
    SINGLE_WRITER_CONFLICT_REASON,
    SingleWriterLockError,
    derive_writer_scope,
)
from polymarket_bot.state_store import StateStore


class SingleWriterLockTests(unittest.TestCase):
    def test_conflicting_writer_lock_uses_standard_reason_code(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            lock_path = Path(tmpdir) / "wallet.lock"
            lock_a = FileLock(str(lock_path), timeout=0.0, writer_scope="paper:default")
            lock_a.acquire()
            try:
                lock_b = FileLock(str(lock_path), timeout=0.0, writer_scope="paper:default")
                with self.assertRaises(SingleWriterLockError) as ctx:
                    lock_b.acquire()
                self.assertEqual(getattr(ctx.exception, "reason_code", ""), SINGLE_WRITER_CONFLICT_REASON)
            finally:
                lock_a.release()

    def test_non_local_lock_path_is_rejected_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            lock_path = Path(tmpdir) / "wallet.lock"
            with patch("polymarket_bot.locks._filesystem_type", return_value=("nfs", None)):
                with self.assertRaises(SingleWriterLockError) as ctx:
                    FileLock(str(lock_path), timeout=0.0, writer_scope="paper:default").acquire()
            self.assertEqual(getattr(ctx.exception, "reason_code", ""), "single_writer_lock_path_non_local")

    def test_derive_writer_scope_rule(self) -> None:
        self.assertEqual(
            derive_writer_scope(dry_run=False, funder_address="0xAbC", watch_wallets=""),
            "live:0xabc",
        )
        self.assertEqual(
            derive_writer_scope(dry_run=True, funder_address="", watch_wallets="0x111,0x222"),
            "paper:0x111",
        )


class StateStoreWriterGuardCoverageTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.calls = 0

        def _guard() -> None:
            self.calls += 1

        self.store = StateStore(str(Path(self.tmpdir.name) / "state.db"), writer_assertion=_guard)
        self.calls = 0

    def tearDown(self) -> None:
        self.tmpdir.cleanup()

    def _assert_guard_called(self, fn) -> None:
        before = self.calls
        fn()
        self.assertGreater(self.calls, before)

    def test_all_mutation_methods_require_writer_guard(self) -> None:
        self._assert_guard_called(lambda: self.store.save_runtime_state({"ts": 1}))
        self._assert_guard_called(lambda: self.store.save_risk_state({"status": "ok"}))
        self._assert_guard_called(lambda: self.store.save_reconciliation_state({"status": "ok"}))
        self._assert_guard_called(lambda: self.store.replace_positions([{"token_id": "token-a", "notional": 1.0}]))
        self._assert_guard_called(
            lambda: self.store.replace_order_intents(
                [
                    {
                        "intent_id": "intent-a",
                        "idempotency_key": "idem-a",
                        "strategy_name": "strategy",
                        "signal_source": "wallet",
                        "signal_fingerprint": "fp-a",
                        "strategy_order_uuid": "so-a",
                        "token_id": "token-a",
                        "condition_id": "condition-a",
                        "side": "BUY",
                        "status": "new",
                        "payload": {},
                    }
                ]
            )
        )
        self._assert_guard_called(lambda: self.store.update_intent_status(status="failed", idempotency_key="idem-a"))
        self._assert_guard_called(
            lambda: self.store.save_runtime_truth(
                {
                    "runtime": {"ts": 1},
                    "control": {"decision_mode": "manual"},
                    "risk": {"status": "ok"},
                    "reconciliation": {"status": "ok"},
                    "positions": [{"token_id": "token-a", "notional": 1.0}],
                    "order_intents": [
                        {
                            "intent_id": "intent-b",
                            "idempotency_key": "idem-b",
                            "strategy_name": "strategy",
                            "signal_source": "wallet",
                            "signal_fingerprint": "fp-b",
                            "strategy_order_uuid": "so-b",
                            "token_id": "token-b",
                            "condition_id": "condition-b",
                            "side": "BUY",
                            "status": "new",
                            "payload": {},
                        }
                    ],
                }
            )
        )
        self._assert_guard_called(
            lambda: self.store.claim_or_load_intent(
                idempotency_key="idem-c",
                intent_id="intent-c",
                token_id="token-c",
                side="BUY",
                status="new",
                payload={},
                strategy_name="strategy",
                signal_source="wallet",
                signal_fingerprint="fp-c",
            )
        )
        self._assert_guard_called(
            lambda: self.store.register_idempotency(
                strategy_order_uuid="so-c",
                wallet="0xabc",
                condition_id="condition-c",
                token_id="token-c",
                side="BUY",
                notional=10.0,
            )
        )
        self._assert_guard_called(lambda: self.store.cleanup_idempotency(window_seconds=1))
        self._assert_guard_called(lambda: self.store.save_control_state({"decision_mode": "manual"}))


class StateStoreWriterGuardFailureTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.store = StateStore(str(Path(self.tmpdir.name) / "state.db"))

        def _raise_not_owner() -> None:
            raise SingleWriterLockError("lost ownership", reason_code="single_writer_ownership_lost")

        self.store._writer_assertion = _raise_not_owner

    def tearDown(self) -> None:
        self.tmpdir.cleanup()

    def _assert_write_blocked(self, fn) -> None:
        with self.assertRaises(SingleWriterLockError) as ctx:
            fn()
        self.assertEqual(getattr(ctx.exception, "reason_code", ""), "single_writer_ownership_lost")

    def test_mutation_methods_fail_when_writer_not_active(self) -> None:
        self._assert_write_blocked(lambda: self.store.save_runtime_state({"ts": 1}))
        self._assert_write_blocked(lambda: self.store.save_risk_state({"status": "ok"}))
        self._assert_write_blocked(lambda: self.store.save_reconciliation_state({"status": "ok"}))
        self._assert_write_blocked(lambda: self.store.replace_positions([{"token_id": "token-a", "notional": 1.0}]))
        self._assert_write_blocked(
            lambda: self.store.replace_order_intents(
                [
                    {
                        "intent_id": "intent-a",
                        "idempotency_key": "idem-a",
                        "strategy_name": "strategy",
                        "signal_source": "wallet",
                        "signal_fingerprint": "fp-a",
                        "strategy_order_uuid": "so-a",
                        "token_id": "token-a",
                        "condition_id": "condition-a",
                        "side": "BUY",
                        "status": "new",
                        "payload": {},
                    }
                ]
            )
        )
        self._assert_write_blocked(lambda: self.store.update_intent_status(status="failed", idempotency_key="idem-a"))
        self._assert_write_blocked(
            lambda: self.store.save_runtime_truth(
                {
                    "runtime": {"ts": 1},
                    "control": {"decision_mode": "manual"},
                    "risk": {"status": "ok"},
                    "reconciliation": {"status": "ok"},
                    "positions": [{"token_id": "token-a", "notional": 1.0}],
                    "order_intents": [
                        {
                            "intent_id": "intent-b",
                            "idempotency_key": "idem-b",
                            "strategy_name": "strategy",
                            "signal_source": "wallet",
                            "signal_fingerprint": "fp-b",
                            "strategy_order_uuid": "so-b",
                            "token_id": "token-b",
                            "condition_id": "condition-b",
                            "side": "BUY",
                            "status": "new",
                            "payload": {},
                        }
                    ],
                }
            )
        )
        self._assert_write_blocked(
            lambda: self.store.claim_or_load_intent(
                idempotency_key="idem-c",
                intent_id="intent-c",
                token_id="token-c",
                side="BUY",
                status="new",
                payload={},
                strategy_name="strategy",
                signal_source="wallet",
                signal_fingerprint="fp-c",
            )
        )
        self._assert_write_blocked(
            lambda: self.store.register_idempotency(
                strategy_order_uuid="so-c",
                wallet="0xabc",
                condition_id="condition-c",
                token_id="token-c",
                side="BUY",
                notional=10.0,
            )
        )
        self._assert_write_blocked(lambda: self.store.cleanup_idempotency(window_seconds=1))
        self._assert_write_blocked(lambda: self.store.save_control_state({"decision_mode": "manual"}))


if __name__ == "__main__":
    unittest.main()
