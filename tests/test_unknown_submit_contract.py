from __future__ import annotations

import time
import unittest

from polymarket_bot.idempotency import (
    CLAIMED_NEW,
    EXISTING_NON_TERMINAL,
    INTENT_STATUS_ACK_UNKNOWN,
    INTENT_STATUS_MANUAL_REQUIRED,
    INTENT_STATUS_NEW,
    INTENT_STATUS_SENDING,
)
from polymarket_bot.runner import Trader
from polymarket_bot.types import ExecutionResult, OpenOrderSnapshot, OrderFillSnapshot
from runtime_persistence_helpers import DummyBroker, DummyDataClient, DummyRisk, DummyStrategy, build_signal, make_settings, new_tmp_dir


def _make_trader(*, workdir, broker: DummyBroker | None = None, dry_run: bool = False) -> Trader:
    settings = make_settings(dry_run=dry_run, workdir=workdir)
    settings.enable_single_writer = False
    settings.decision_mode = "auto"
    settings.ack_unknown_recovery_window_seconds = 300
    settings.ack_unknown_max_probes = 2
    return Trader(
        settings=settings,
        data_client=DummyDataClient(),
        strategy=DummyStrategy([]),
        risk=DummyRisk(),
        broker=broker or DummyBroker(),
    )


class UnknownSubmitContractTests(unittest.TestCase):
    def _register_unknown_submit_pending(
        self,
        *,
        trader: Trader,
        signal,
        submit_digest: str = "digest-1",
        probe_confidence: str = "weak",
        probe_basis: str = "submit_digest_only",
        manual_required_reason: str = "",
        first_seen_ts: int | None = None,
        probe_count: int = 1,
    ) -> dict[str, object]:
        first_seen = int(first_seen_ts or time.time())
        result = ExecutionResult(
            ok=True,
            broker_order_id=None,
            message="ack unknown",
            filled_notional=0.0,
            filled_price=0.0,
            status=INTENT_STATUS_ACK_UNKNOWN,
            requested_notional=15.0,
            requested_price=float(signal.price_hint or 0.0),
            metadata={
                "pending_class": "submit_unknown",
                "submit_digest": submit_digest,
                "submit_digest_version": "sdig-v1",
                "probe_confidence": probe_confidence,
                "probe_basis": probe_basis,
                "unknown_submit_first_seen_ts": first_seen,
                "unknown_submit_probe_count": probe_count,
                "manual_required_reason": manual_required_reason,
                "submitted_price": float(signal.price_hint or 0.0),
                "submitted_size": 1.0,
                "tick_size": 0.01,
            },
        )
        pending = trader._register_pending_order(
            signal=signal,
            cycle_id="cycle-1",
            result=result,
            order_meta={"strategy_order_uuid": "so-test", "idempotency_key": "ik-test"},
            entry_context={},
            previous_position={},
            order_reason="test",
            now=first_seen,
        )
        trader._apply_submit_unknown_contract(
            pending,
            now=first_seen,
            probe_confidence=probe_confidence,
            probe_basis=probe_basis,
            manual_required_reason=manual_required_reason,
            ack_state={"count": probe_count, "first_ts": first_seen},
            payload=dict(result.metadata),
        )
        return pending

    def test_submit_without_broker_id_isolated(self):
        trader = _make_trader(workdir=new_tmp_dir(), dry_run=False)
        signal = build_signal(token_id="token-isolated", side="BUY")
        pending = self._register_unknown_submit_pending(trader=trader, signal=signal)

        self.assertEqual(pending["key"], trader._pending_order_key(signal, None))
        self.assertEqual(str(pending["pending_class"]), "submit_unknown")
        self.assertEqual(str(pending["recovery_status"]), INTENT_STATUS_ACK_UNKNOWN)
        self.assertEqual(str(pending["probe_confidence"]), "weak")
        self.assertEqual(str(pending["probe_basis"]), "submit_digest_only")
        self.assertEqual(str(pending["submit_digest_version"]), "sdig-v1")

    def test_submit_restart_probe_strong_evidence_recovers_same_intent(self):
        first_seen = int(time.time())
        signal = build_signal(token_id="token-strong", side="BUY")
        broker = DummyBroker(
            open_orders=[
                OpenOrderSnapshot(
                    order_id="oid-strong",
                    token_id=str(signal.token_id),
                    side="BUY",
                    status="live",
                    price=float(signal.price_hint),
                    original_size=1.0,
                    remaining_size=1.0,
                    created_ts=first_seen,
                    condition_id=str(signal.condition_id),
                    market_slug=str(signal.market_slug),
                )
            ]
        )
        trader = _make_trader(workdir=new_tmp_dir(), broker=broker, dry_run=False)

        probe = trader._classify_unknown_submit_probe(
            signal=signal,
            intent_record={
                "payload": {
                    "submitted_price": float(signal.price_hint),
                    "submitted_size": 1.0,
                    "tick_size": 0.01,
                    "unknown_submit_first_seen_ts": first_seen,
                    "submit_digest": "digest-strong",
                }
            },
        )

        self.assertEqual(str(probe["confidence"]), "strong")
        self.assertEqual(str(probe["basis"]), "unique_broker_record_match")
        self.assertEqual(str(probe["broker_order_id"]), "oid-strong")

    def test_submit_digest_is_weak_anchor_only(self):
        signal = build_signal(token_id="token-digest", side="BUY")
        trader = _make_trader(workdir=new_tmp_dir(), broker=DummyBroker(), dry_run=False)

        probe = trader._classify_unknown_submit_probe(
            signal=signal,
            intent_record={"payload": {"submit_digest": "digest-only", "submit_digest_version": "sdig-v1"}},
        )

        self.assertEqual(str(probe["confidence"]), "weak")
        self.assertEqual(str(probe["basis"]), "submit_digest_only")
        self.assertEqual(str(probe["intent_status"]), INTENT_STATUS_ACK_UNKNOWN)

    def test_submit_unknown_weak_evidence_stays_blocked(self):
        first_seen = int(time.time())
        signal = build_signal(token_id="token-weak", side="BUY")
        broker = DummyBroker(
            open_orders=[
                OpenOrderSnapshot(
                    order_id="oid-weak",
                    token_id=str(signal.token_id),
                    side="BUY",
                    status="live",
                    price=float(signal.price_hint),
                    original_size=1.0,
                    remaining_size=1.0,
                    created_ts=first_seen,
                    condition_id=str(signal.condition_id),
                    market_slug=str(signal.market_slug),
                )
            ]
        )
        trader = _make_trader(workdir=new_tmp_dir(), broker=broker, dry_run=False)
        probe = trader._classify_unknown_submit_probe(
            signal=signal,
            intent_record={
                "payload": {
                    "submitted_price": float(signal.price_hint),
                    "submitted_size": 1.0,
                    "tick_size": 0.0,
                    "unknown_submit_first_seen_ts": first_seen,
                    "submit_digest": "digest-weak",
                }
            },
        )
        pending = self._register_unknown_submit_pending(
            trader=trader,
            signal=signal,
            submit_digest="digest-weak",
            probe_confidence=str(probe["confidence"]),
            probe_basis=str(probe["basis"]),
            first_seen_ts=first_seen,
        )

        self.assertEqual(str(probe["confidence"]), "weak")
        self.assertEqual(str(pending["pending_class"]), "submit_unknown")
        self.assertEqual(str(pending["recovery_status"]), INTENT_STATUS_ACK_UNKNOWN)
        self.assertEqual(trader._intent_pending_status_from_order(pending), INTENT_STATUS_ACK_UNKNOWN)

    def test_submit_unknown_none_evidence_escalates_manual_required(self):
        signal = build_signal(token_id="token-none", side="BUY")
        trader = _make_trader(workdir=new_tmp_dir(), broker=DummyBroker(), dry_run=False)
        pending = self._register_unknown_submit_pending(
            trader=trader,
            signal=signal,
            submit_digest="",
            probe_confidence="none",
            probe_basis="no_match",
            manual_required_reason="submit_unknown_no_anchor",
        )

        self.assertEqual(str(pending["recovery_status"]), INTENT_STATUS_MANUAL_REQUIRED)
        self.assertEqual(str(pending["manual_required_reason"]), "submit_unknown_no_anchor")
        self.assertEqual(trader._intent_pending_status_from_order(pending), INTENT_STATUS_MANUAL_REQUIRED)

    def test_submit_unknown_multiple_anchored_matches_are_ambiguous(self):
        first_seen = int(time.time())
        signal = build_signal(token_id="token-conflict", side="BUY")
        broker = DummyBroker(
            open_orders=[
                OpenOrderSnapshot(
                    order_id="oid-conflict-open",
                    token_id=str(signal.token_id),
                    side="BUY",
                    status="live",
                    price=float(signal.price_hint),
                    original_size=1.0,
                    remaining_size=1.0,
                    created_ts=first_seen,
                    condition_id=str(signal.condition_id),
                    market_slug=str(signal.market_slug),
                )
            ],
            fills=[
                OrderFillSnapshot(
                    order_id="oid-conflict-fill",
                    token_id=str(signal.token_id),
                    side="BUY",
                    price=float(signal.price_hint),
                    size=1.0,
                    timestamp=first_seen,
                    market_slug=str(signal.market_slug),
                )
            ],
        )
        trader = _make_trader(workdir=new_tmp_dir(), broker=broker, dry_run=False)

        probe = trader._classify_unknown_submit_probe(
            signal=signal,
            intent_record={
                "payload": {
                    "submitted_price": float(signal.price_hint),
                    "submitted_size": 1.0,
                    "tick_size": 0.01,
                    "unknown_submit_first_seen_ts": first_seen,
                    "submit_digest": "digest-conflict",
                }
            },
        )

        self.assertEqual(str(probe["confidence"]), "weak")
        self.assertEqual(str(probe["basis"]), "ambiguous_broker_record_match")
        self.assertEqual(str(probe["manual_required_reason"]), "submit_unknown_ambiguous_match")

    def test_submit_unknown_unanchored_same_token_activity_falls_back_to_digest(self):
        first_seen = int(time.time())
        signal = build_signal(token_id="token-unanchored", side="BUY")
        broker = DummyBroker(
            open_orders=[
                OpenOrderSnapshot(
                    order_id="oid-stale-open",
                    token_id=str(signal.token_id),
                    side="BUY",
                    status="live",
                    price=float(signal.price_hint),
                    original_size=1.0,
                    remaining_size=1.0,
                    created_ts=first_seen - 3600,
                    condition_id=str(signal.condition_id),
                    market_slug=str(signal.market_slug),
                )
            ],
            fills=[
                OrderFillSnapshot(
                    order_id="oid-wrong-fill",
                    token_id=str(signal.token_id),
                    side="BUY",
                    price=float(signal.price_hint) + 0.02,
                    size=2.0,
                    timestamp=first_seen,
                    market_slug=str(signal.market_slug),
                )
            ],
        )
        trader = _make_trader(workdir=new_tmp_dir(), broker=broker, dry_run=False)

        probe = trader._classify_unknown_submit_probe(
            signal=signal,
            intent_record={
                "payload": {
                    "submitted_price": float(signal.price_hint),
                    "submitted_size": 1.0,
                    "tick_size": 0.01,
                    "unknown_submit_first_seen_ts": first_seen,
                    "submit_digest": "digest-unanchored",
                }
            },
        )

        self.assertEqual(str(probe["confidence"]), "weak")
        self.assertEqual(str(probe["basis"]), "submit_digest_only")
        self.assertEqual(str(probe["manual_required_reason"]), "")

    def test_existing_manual_required_blocks_new_intent_and_resend(self):
        workdir = new_tmp_dir()
        trader = _make_trader(workdir=workdir, dry_run=True)
        signal = build_signal(token_id="token-manual-block", side="BUY")
        identity = trader._build_intent_identity(signal, 19.0)

        claim_status, _ = trader._claim_or_load_intent(
            signal=signal,
            notional_usd=19.0,
            identity=identity,
        )
        self.assertEqual(claim_status, CLAIMED_NEW)

        locked, _ = trader._set_intent_status(
            strategy_order_uuid=str(identity["strategy_order_uuid"]),
            idempotency_key=str(identity["idempotency_key"]),
            status=INTENT_STATUS_MANUAL_REQUIRED,
        )
        self.assertTrue(locked)

        second_status, second_intent = trader._claim_or_load_intent(
            signal=signal,
            notional_usd=19.0,
            identity=identity,
        )
        self.assertEqual(second_status, EXISTING_NON_TERMINAL)
        self.assertEqual(str(second_intent.get("status") or ""), INTENT_STATUS_MANUAL_REQUIRED)

        resend_allowed, _ = trader._set_intent_status(
            strategy_order_uuid=str(identity["strategy_order_uuid"]),
            idempotency_key=str(identity["idempotency_key"]),
            status=INTENT_STATUS_SENDING,
            expected_from_statuses=(INTENT_STATUS_NEW,),
        )
        self.assertFalse(resend_allowed)

    def test_submit_unknown_contract_is_consumed_by_patch_02(self):
        trader = _make_trader(workdir=new_tmp_dir(), dry_run=True)
        row = {
            "key": "sig-contract:BUY:token-contract",
            "ts": int(time.time()),
            "order_id": "",
            "broker_status": "posted",
            "signal_id": "sig-contract",
            "token_id": "token-contract",
            "condition_id": "condition-token-contract",
            "market_slug": "demo-market",
            "outcome": "YES",
            "side": "BUY",
            "requested_notional": 15.0,
            "requested_price": 0.55,
            "strategy_order_uuid": "so-contract",
            "idempotency_key": "ik-contract",
            "recovery_status": INTENT_STATUS_MANUAL_REQUIRED,
            "pending_class": "submit_unknown",
            "submit_digest": "digest-contract",
            "submit_digest_version": "sdig-v1",
            "probe_confidence": "weak",
            "probe_basis": "submit_digest_only",
            "unknown_submit_first_seen_ts": int(time.time()),
            "unknown_submit_probe_count": 2,
            "manual_required_reason": "submit_unknown_probe_exhausted",
            "ack_unknown_count": 2,
            "ack_unknown_first_ts": int(time.time()),
            "submitted_price": 0.55,
            "submitted_size": 1.0,
        }

        restored = trader._restore_pending_order(row)
        self.assertIsNotNone(restored)
        assert restored is not None
        self.assertEqual(str(restored["pending_class"]), "submit_unknown")
        self.assertEqual(str(restored["probe_confidence"]), "weak")
        self.assertEqual(str(restored["manual_required_reason"]), "submit_unknown_probe_exhausted")
        self.assertEqual(trader._intent_pending_status_from_order(restored), INTENT_STATUS_MANUAL_REQUIRED)


if __name__ == "__main__":
    unittest.main()
