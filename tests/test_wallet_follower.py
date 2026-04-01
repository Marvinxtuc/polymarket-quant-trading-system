from __future__ import annotations

import unittest
from datetime import datetime, timezone
import time

from polymarket_bot.clients.data_api import ActivityEvent, MarketMetadata, Position, TradeFill
from polymarket_bot.strategies.wallet_follower import (
    PositionState,
    RawWalletEvent,
    WalletCandidateContext,
    WalletFollowerStrategy,
)


class WalletFollowerEventSignalTests(unittest.TestCase):
    def test_detect_wallet_events_dedupes_same_fill_across_sources(self):
        wallet = "0x00000000000000000000000000000000000000a0"
        event_ts = int(datetime.now(tz=timezone.utc).timestamp()) + 5

        class _Client:
            def get_active_positions(self, _wallet: str, limit: int = 200):
                return []

            def get_user_trades(self, _wallet: str, *, limit: int = 1000, offset: int = 0, **_kwargs):
                if offset > 0:
                    return []
                return [
                    TradeFill(
                        wallet=wallet,
                        side="BUY",
                        token_id="token-raw",
                        condition_id="condition-raw",
                        market_slug="will-btc-close-above-100k",
                        outcome="YES",
                        price=0.61,
                        size=500.0,
                        timestamp=event_ts,
                        tx_hash="0xdup",
                    )
                ]

            def get_user_activity(self, _wallet: str, *, limit: int = 500, offset: int = 0, **_kwargs):
                if offset > 0:
                    return []
                return [
                    ActivityEvent(
                        wallet=wallet,
                        activity_type="TRADE",
                        token_id="token-raw",
                        condition_id="condition-raw",
                        market_slug="will-btc-close-above-100k",
                        outcome="YES",
                        side="BUY",
                        price=0.61,
                        size=500.0,
                        usdc_size=305.0,
                        timestamp=event_ts,
                        tx_hash="0xdup",
                    )
                ]

        strategy = WalletFollowerStrategy(
            client=_Client(),
            min_increase_usd=300.0,
            min_decrease_usd=200.0,
            max_signals_per_cycle=5,
            min_active_positions=1,
            min_unique_markets=1,
            min_total_notional_usd=0.0,
            max_top_market_share=1.0,
            min_wallet_score=0.0,
            signal_source="hybrid",
        )

        events = strategy.detect_wallet_events(wallet, warmup=False)

        self.assertEqual(len(events), 1)
        self.assertIsInstance(events[0], RawWalletEvent)
        self.assertEqual(events[0].token_id, "token-raw")
        self.assertEqual(events[0].side, "BUY")
        self.assertEqual(events[0].dedupe_key, "0xdup:token-raw:BUY")

    def test_build_candidates_assigns_new_open_add_readd_and_exit_triggers(self):
        wallet = "0x00000000000000000000000000000000000000a6"

        class _Client:
            def get_active_positions(self, _wallet: str, limit: int = 200):
                return []

            def get_user_trades(self, _wallet: str, *, limit: int = 1000, offset: int = 0, **_kwargs):
                return []

            def get_user_activity(self, _wallet: str, *, limit: int = 500, offset: int = 0, **_kwargs):
                return []

        strategy = WalletFollowerStrategy(
            client=_Client(),
            min_increase_usd=100.0,
            min_decrease_usd=80.0,
            follow_wallet_exits=True,
            max_signals_per_cycle=5,
            min_active_positions=1,
            min_unique_markets=1,
            min_total_notional_usd=0.0,
            max_top_market_share=1.0,
            min_wallet_score=0.0,
            signal_source="positions",
        )

        strategy._wallet_seen_tokens[wallet] = {"token-readd"}
        wallet_state = {
            "token-add": PositionState(
                size=500.0,
                notional=250.0,
                price=0.50,
                updated_ts=1,
                condition_id="condition-add",
                market_slug="add-market",
                outcome="YES",
            ),
            "token-exit": PositionState(
                size=400.0,
                notional=240.0,
                price=0.60,
                updated_ts=1,
                condition_id="condition-exit",
                market_slug="exit-market",
                outcome="YES",
            ),
        }
        latest_positions = [
            Position(
                wallet=wallet,
                token_id="token-new",
                condition_id="condition-new",
                market_slug="new-market",
                outcome="YES",
                avg_price=0.55,
                size=300.0,
                notional=165.0,
                timestamp=2,
            ),
            Position(
                wallet=wallet,
                token_id="token-add",
                condition_id="condition-add",
                market_slug="add-market",
                outcome="YES",
                avg_price=0.56,
                size=900.0,
                notional=450.0,
                timestamp=3,
            ),
            Position(
                wallet=wallet,
                token_id="token-readd",
                condition_id="condition-readd",
                market_slug="readd-market",
                outcome="YES",
                avg_price=0.58,
                size=700.0,
                notional=406.0,
                timestamp=4,
            ),
        ]

        candidates = strategy.build_candidates(
            wallet,
            latest_positions,
            wallet_state,
            {
                "wallet_score": 72.0,
                "wallet_tier": "CORE",
                "score_summary": "core wallet",
                "topic_profiles": [],
            },
            [],
            wallet_is_eligible=True,
            is_warmup_cycle=False,
        )

        by_token = {candidate.token_id: candidate for candidate in candidates}

        self.assertEqual(by_token["token-new"].trigger_type, "new_open")
        self.assertEqual(by_token["token-add"].trigger_type, "add")
        self.assertEqual(by_token["token-readd"].trigger_type, "readd")

        exit_candidates = [candidate for candidate in candidates if candidate.token_id == "token-exit"]
        self.assertEqual(len(exit_candidates), 1)
        self.assertEqual(exit_candidates[0].side, "SELL")
        self.assertEqual(exit_candidates[0].trigger_type, "exit")

    def test_rank_candidates_combines_multi_wallet_buy_resonance(self):
        now = datetime.now(tz=timezone.utc)
        later = datetime.fromtimestamp(int(now.timestamp()) + 60, tz=timezone.utc)
        candidate_a = WalletCandidateContext(
            wallet="0x00000000000000000000000000000000000000b1",
            token_id="token-r",
            condition_id="condition-r",
            market_slug="resonance-market",
            outcome="YES",
            side="BUY",
            trigger_type="new_open",
            confidence=0.71,
            price_hint=0.60,
            observed_size=100.0,
            observed_notional=60.0,
            timestamp=now,
            wallet_score=72.0,
            wallet_tier="CORE",
            wallet_score_summary="wallet-a",
            topic_key="macro",
            topic_label="Macro",
            topic_sample_count=8,
            topic_win_rate=0.62,
            topic_roi=0.14,
            topic_resolved_win_rate=0.65,
            topic_score_summary="macro summary",
            position_action="entry",
            position_action_label="首次入场",
        )
        candidate_b = WalletCandidateContext(
            wallet="0x00000000000000000000000000000000000000b2",
            token_id="token-r",
            condition_id="condition-r",
            market_slug="resonance-market",
            outcome="YES",
            side="BUY",
            trigger_type="add",
            confidence=0.80,
            price_hint=0.61,
            observed_size=140.0,
            observed_notional=85.0,
            timestamp=later,
            wallet_score=81.0,
            wallet_tier="CORE",
            wallet_score_summary="wallet-b",
            topic_key="macro",
            topic_label="Macro",
            topic_sample_count=8,
            topic_win_rate=0.62,
            topic_roi=0.14,
            topic_resolved_win_rate=0.65,
            topic_score_summary="macro summary",
            position_action="add",
            position_action_label="追加买入",
        )

        strategy = WalletFollowerStrategy(
            client=object(),
            min_increase_usd=100.0,
            min_decrease_usd=80.0,
            max_signals_per_cycle=5,
            min_active_positions=1,
            min_unique_markets=1,
            min_total_notional_usd=0.0,
            max_top_market_share=1.0,
            min_wallet_score=0.0,
            signal_source="positions",
        )

        signals = strategy.rank_candidates([candidate_a, candidate_b])

        self.assertEqual(len(signals), 1)
        signal = signals[0]
        self.assertEqual(signal.wallet, candidate_b.wallet)
        self.assertEqual(signal.position_action_label, candidate_b.position_action_label)
        self.assertEqual(signal.side, "BUY")
        self.assertEqual(signal.topic_key, "macro")
        self.assertIn("buy resonance observed (2 wallets)", signal.wallet_score_summary)
        self.assertAlmostEqual(signal.confidence, candidate_b.confidence, places=6)
        self.assertAlmostEqual(signal.observed_notional, candidate_b.observed_notional, places=4)
        self.assertAlmostEqual(signal.price_hint, candidate_b.price_hint, places=6)
        self.assertEqual(signal.timestamp, later)

    def test_select_live_signals_skips_buy_without_live_orderbook_and_backfills_next_ranked_buy(self):
        wallet = "0x00000000000000000000000000000000000000bf"
        now = datetime.now(tz=timezone.utc)

        class _Book:
            def __init__(self, best_bid: float, best_ask: float) -> None:
                self.best_bid = best_bid
                self.best_ask = best_ask

        class _Client:
            def get_order_book(self, token_id: str):
                if token_id in {"token-invalid-a", "token-invalid-b"}:
                    return None
                return _Book(0.54, 0.55)

        def make_candidate(token_id: str, wallet_score: float) -> WalletCandidateContext:
            return WalletCandidateContext(
                wallet=wallet,
                token_id=token_id,
                condition_id=f"condition-{token_id}",
                market_slug=f"market-{token_id}",
                outcome="YES",
                side="BUY",
                trigger_type="new_open",
                confidence=0.82,
                price_hint=0.52,
                observed_size=200.0,
                observed_notional=104.0,
                timestamp=now,
                wallet_score=wallet_score,
                wallet_tier="CORE",
                wallet_score_summary=f"score-{wallet_score}",
                topic_key="macro",
                topic_label="Macro",
                topic_sample_count=4,
                topic_win_rate=0.6,
                topic_roi=0.1,
                topic_resolved_win_rate=0.6,
                topic_score_summary="macro summary",
                position_action="entry",
                position_action_label="首次入场",
            )

        strategy = WalletFollowerStrategy(
            client=_Client(),
            min_increase_usd=100.0,
            min_decrease_usd=80.0,
            max_signals_per_cycle=2,
            min_active_positions=1,
            min_unique_markets=1,
            min_total_notional_usd=0.0,
            max_top_market_share=1.0,
            min_wallet_score=0.0,
            signal_source="positions",
        )

        ranked_candidates = [
            make_candidate("token-invalid-a", 95.0),
            make_candidate("token-invalid-b", 90.0),
            make_candidate("token-valid-a", 85.0),
            make_candidate("token-valid-b", 80.0),
        ]

        ranked_signals = strategy.rank_candidates(ranked_candidates)
        signals = strategy._select_live_signals(ranked_signals)

        self.assertEqual([signal.token_id for signal in signals], ["token-valid-a", "token-valid-b"])
        self.assertTrue(all(signal.side == "BUY" for signal in signals))
        self.assertEqual(len(signals), 2)

    def test_select_live_signals_skips_short_window_edge_price_buy(self):
        wallet = "0x00000000000000000000000000000000000000c0"
        now = datetime.now(tz=timezone.utc)
        market_start = int(time.time()) - 30

        class _Book:
            def __init__(self, best_bid: float, best_ask: float) -> None:
                self.best_bid = best_bid
                self.best_ask = best_ask

        class _Client:
            def get_order_book(self, token_id: str):
                if token_id == "token-edge":
                    return _Book(0.01, 0.99)
                return _Book(0.54, 0.55)

        def make_candidate(token_id: str, market_slug: str, wallet_score: float, price_hint: float) -> WalletCandidateContext:
            return WalletCandidateContext(
                wallet=wallet,
                token_id=token_id,
                condition_id=f"condition-{token_id}",
                market_slug=market_slug,
                outcome="YES",
                side="BUY",
                trigger_type="new_open",
                confidence=0.82,
                price_hint=price_hint,
                observed_size=200.0,
                observed_notional=104.0,
                timestamp=now,
                wallet_score=wallet_score,
                wallet_tier="CORE",
                wallet_score_summary=f"score-{wallet_score}",
                topic_key="macro",
                topic_label="Macro",
                topic_sample_count=4,
                topic_win_rate=0.6,
                topic_roi=0.1,
                topic_resolved_win_rate=0.6,
                topic_score_summary="macro summary",
                position_action="entry",
                position_action_label="首次入场",
            )

        strategy = WalletFollowerStrategy(
            client=_Client(),
            min_increase_usd=100.0,
            min_decrease_usd=80.0,
            max_signals_per_cycle=2,
            min_active_positions=1,
            min_unique_markets=1,
            min_total_notional_usd=0.0,
            max_top_market_share=1.0,
            min_wallet_score=0.0,
            signal_source="positions",
        )

        ranked_signals = strategy.rank_candidates(
            [
                make_candidate("token-edge", f"btc-updown-5m-{market_start}", 95.0, 0.52),
                make_candidate("token-valid", "macro-market", 85.0, 0.52),
            ]
        )
        signals = strategy._select_live_signals(ranked_signals)

        self.assertEqual([signal.token_id for signal in signals], ["token-valid"])

    def test_select_live_signals_skips_short_window_late_buy(self):
        wallet = "0x00000000000000000000000000000000000000c1"
        now = datetime.now(tz=timezone.utc)
        market_start = int(time.time()) - 250

        class _Book:
            def __init__(self, best_bid: float, best_ask: float) -> None:
                self.best_bid = best_bid
                self.best_ask = best_ask

        class _Client:
            def get_order_book(self, _token_id: str):
                return _Book(0.54, 0.55)

        def make_candidate(token_id: str, market_slug: str, wallet_score: float) -> WalletCandidateContext:
            return WalletCandidateContext(
                wallet=wallet,
                token_id=token_id,
                condition_id=f"condition-{token_id}",
                market_slug=market_slug,
                outcome="YES",
                side="BUY",
                trigger_type="new_open",
                confidence=0.82,
                price_hint=0.52,
                observed_size=200.0,
                observed_notional=104.0,
                timestamp=now,
                wallet_score=wallet_score,
                wallet_tier="CORE",
                wallet_score_summary=f"score-{wallet_score}",
                topic_key="macro",
                topic_label="Macro",
                topic_sample_count=4,
                topic_win_rate=0.6,
                topic_roi=0.1,
                topic_resolved_win_rate=0.6,
                topic_score_summary="macro summary",
                position_action="entry",
                position_action_label="首次入场",
            )

        strategy = WalletFollowerStrategy(
            client=_Client(),
            min_increase_usd=100.0,
            min_decrease_usd=80.0,
            max_signals_per_cycle=2,
            min_active_positions=1,
            min_unique_markets=1,
            min_total_notional_usd=0.0,
            max_top_market_share=1.0,
            min_wallet_score=0.0,
            signal_source="positions",
        )

        ranked_signals = strategy.rank_candidates(
            [
                make_candidate("token-late", f"eth-updown-5m-{market_start}", 95.0),
                make_candidate("token-valid", "macro-market", 85.0),
            ]
        )
        signals = strategy._select_live_signals(ranked_signals)

        self.assertEqual([signal.token_id for signal in signals], ["token-valid"])

    def test_select_live_signals_skips_market_not_accepting_orders_from_metadata(self):
        wallet = "0x00000000000000000000000000000000000000c1"
        now = datetime.now(tz=timezone.utc)

        class _Book:
            def __init__(self, best_bid: float, best_ask: float) -> None:
                self.best_bid = best_bid
                self.best_ask = best_ask

        class _Client:
            def get_order_book(self, _token_id: str):
                return _Book(0.54, 0.55)

            def get_market_metadata(self, condition_id: str = "", *, slug: str | None = None):
                if condition_id == "condition-token-blocked":
                    return MarketMetadata(
                        condition_id="condition-token-blocked",
                        market_slug="metadata-blocked-market",
                        end_ts=int(time.time()) + 3600,
                        end_date="2026-03-22T12:00:00Z",
                        closed=False,
                        active=True,
                        accepting_orders=False,
                    )
                return None

        def make_candidate(token_id: str, condition_id: str, market_slug: str, wallet_score: float) -> WalletCandidateContext:
            return WalletCandidateContext(
                wallet=wallet,
                token_id=token_id,
                condition_id=condition_id,
                market_slug=market_slug,
                outcome="YES",
                side="BUY",
                trigger_type="new_open",
                confidence=0.82,
                price_hint=0.52,
                observed_size=200.0,
                observed_notional=104.0,
                timestamp=now,
                wallet_score=wallet_score,
                wallet_tier="CORE",
                wallet_score_summary=f"score-{wallet_score}",
                topic_key="macro",
                topic_label="Macro",
                topic_sample_count=4,
                topic_win_rate=0.6,
                topic_roi=0.1,
                topic_resolved_win_rate=0.6,
                topic_score_summary="macro summary",
                position_action="entry",
                position_action_label="首次入场",
            )

        strategy = WalletFollowerStrategy(
            client=_Client(),
            min_increase_usd=100.0,
            min_decrease_usd=80.0,
            max_signals_per_cycle=2,
            min_active_positions=1,
            min_unique_markets=1,
            min_total_notional_usd=0.0,
            max_top_market_share=1.0,
            min_wallet_score=0.0,
            signal_source="positions",
        )

        ranked_signals = strategy.rank_candidates(
            [
                make_candidate("token-blocked", "condition-token-blocked", "metadata-blocked-market", 95.0),
                make_candidate("token-valid", "condition-token-valid", "macro-market", 85.0),
            ]
        )
        signals = strategy._select_live_signals(ranked_signals)

        self.assertEqual([signal.token_id for signal in signals], ["token-valid"])

    def test_select_live_signals_skips_elapsed_market_from_metadata(self):
        wallet = "0x00000000000000000000000000000000000000c1"
        now = datetime.now(tz=timezone.utc)

        class _Book:
            def __init__(self, best_bid: float, best_ask: float) -> None:
                self.best_bid = best_bid
                self.best_ask = best_ask

        class _Client:
            def get_order_book(self, _token_id: str):
                return _Book(0.54, 0.55)

            def get_market_metadata(self, condition_id: str = "", *, slug: str | None = None):
                if condition_id == "condition-token-expired":
                    return MarketMetadata(
                        condition_id="condition-token-expired",
                        market_slug="metadata-expired-market",
                        end_ts=int(time.time()) - 5,
                        end_date="2026-03-20T23:59:00Z",
                        closed=False,
                        active=True,
                        accepting_orders=True,
                    )
                return None

        def make_candidate(token_id: str, condition_id: str, market_slug: str, wallet_score: float) -> WalletCandidateContext:
            return WalletCandidateContext(
                wallet=wallet,
                token_id=token_id,
                condition_id=condition_id,
                market_slug=market_slug,
                outcome="YES",
                side="BUY",
                trigger_type="new_open",
                confidence=0.82,
                price_hint=0.52,
                observed_size=200.0,
                observed_notional=104.0,
                timestamp=now,
                wallet_score=wallet_score,
                wallet_tier="CORE",
                wallet_score_summary=f"score-{wallet_score}",
                topic_key="macro",
                topic_label="Macro",
                topic_sample_count=4,
                topic_win_rate=0.6,
                topic_roi=0.1,
                topic_resolved_win_rate=0.6,
                topic_score_summary="macro summary",
                position_action="entry",
                position_action_label="首次入场",
            )

        strategy = WalletFollowerStrategy(
            client=_Client(),
            min_increase_usd=100.0,
            min_decrease_usd=80.0,
            max_signals_per_cycle=2,
            min_active_positions=1,
            min_unique_markets=1,
            min_total_notional_usd=0.0,
            max_top_market_share=1.0,
            min_wallet_score=0.0,
            signal_source="positions",
        )

        ranked_signals = strategy.rank_candidates(
            [
                make_candidate("token-expired", "condition-token-expired", "metadata-expired-market", 95.0),
                make_candidate("token-valid", "condition-token-valid", "macro-market", 85.0),
            ]
        )
        signals = strategy._select_live_signals(ranked_signals)

        self.assertEqual([signal.token_id for signal in signals], ["token-valid"])

    def test_select_live_signals_skips_long_window_high_chase_buy(self):
        wallet = "0x00000000000000000000000000000000000000c2"
        now = datetime.now(tz=timezone.utc)

        class _Book:
            def __init__(self, best_bid: float, best_ask: float) -> None:
                self.best_bid = best_bid
                self.best_ask = best_ask

        class _Client:
            def get_order_book(self, token_id: str):
                if token_id == "token-edge":
                    return _Book(0.01, 0.99)
                return _Book(0.54, 0.55)

        def make_candidate(token_id: str, market_slug: str, wallet_score: float, price_hint: float) -> WalletCandidateContext:
            return WalletCandidateContext(
                wallet=wallet,
                token_id=token_id,
                condition_id=f"condition-{token_id}",
                market_slug=market_slug,
                outcome="YES",
                side="BUY",
                trigger_type="new_open",
                confidence=0.82,
                price_hint=price_hint,
                observed_size=200.0,
                observed_notional=104.0,
                timestamp=now,
                wallet_score=wallet_score,
                wallet_tier="CORE",
                wallet_score_summary=f"score-{wallet_score}",
                topic_key="macro",
                topic_label="Macro",
                topic_sample_count=4,
                topic_win_rate=0.6,
                topic_roi=0.1,
                topic_resolved_win_rate=0.6,
                topic_score_summary="macro summary",
                position_action="entry",
                position_action_label="首次入场",
            )

        strategy = WalletFollowerStrategy(
            client=_Client(),
            min_increase_usd=100.0,
            min_decrease_usd=80.0,
            max_signals_per_cycle=2,
            min_active_positions=1,
            min_unique_markets=1,
            min_total_notional_usd=0.0,
            max_top_market_share=1.0,
            min_wallet_score=0.0,
            signal_source="positions",
        )

        ranked_signals = strategy.rank_candidates(
            [
                make_candidate("token-edge", "bitcoin-up-or-down-march-19-2026-6am-et", 95.0, 0.52),
                make_candidate("token-valid", "macro-market", 85.0, 0.52),
            ]
        )
        signals = strategy._select_live_signals(ranked_signals)

        self.assertEqual([signal.token_id for signal in signals], ["token-valid"])

    def test_hybrid_mode_prefers_trade_event_and_dedupes_snapshot_buy(self):
        wallet = "0x00000000000000000000000000000000000000a1"
        event_ts = int(datetime.now(tz=timezone.utc).timestamp()) + 5

        class _Client:
            def __init__(self) -> None:
                self.position_calls = 0
                self.trade_calls = 0

            def get_active_positions(self, _wallet: str, limit: int = 200):
                self.position_calls += 1
                if self.position_calls == 1:
                    return [
                        Position(
                            wallet=wallet,
                            token_id="token-a",
                            market_slug="will-btc-close-above-100k",
                            outcome="YES",
                            avg_price=0.60,
                            size=1000.0,
                            notional=600.0,
                            timestamp=1,
                        )
                    ]
                return [
                    Position(
                        wallet=wallet,
                        token_id="token-a",
                        market_slug="will-btc-close-above-100k",
                        outcome="YES",
                        avg_price=0.60,
                        size=1500.0,
                        notional=900.0,
                        timestamp=2,
                    )
                ]

            def get_user_trades(self, _wallet: str, *, limit: int = 1000, offset: int = 0, **_kwargs):
                self.trade_calls += 1
                if self.trade_calls == 1 or offset > 0:
                    return []
                return [
                    TradeFill(
                        wallet=wallet,
                        side="BUY",
                        token_id="token-a",
                        condition_id="condition-a",
                        market_slug="will-btc-close-above-100k",
                        outcome="YES",
                        price=0.62,
                        size=500.0,
                        timestamp=event_ts,
                        tx_hash="0xtrade-buy",
                    )
                ]

            def get_user_activity(self, _wallet: str, *, limit: int = 500, offset: int = 0, **_kwargs):
                return []

        strategy = WalletFollowerStrategy(
            client=_Client(),
            min_increase_usd=300.0,
            min_decrease_usd=200.0,
            max_signals_per_cycle=5,
            min_active_positions=1,
            min_unique_markets=1,
            min_total_notional_usd=0.0,
            max_top_market_share=1.0,
            min_wallet_score=0.0,
            signal_source="hybrid",
        )
        strategy.update_wallet_activity_counts({wallet: 10})

        self.assertEqual(strategy.generate_signals([wallet]), [])

        signals = strategy.generate_signals([wallet])

        self.assertEqual(len(signals), 1)
        self.assertEqual(signals[0].side, "BUY")
        self.assertEqual(signals[0].condition_id, "condition-a")
        self.assertEqual(signals[0].position_action_label, "事件加仓")
        self.assertAlmostEqual(signals[0].price_hint, 0.62, places=4)
        self.assertAlmostEqual(signals[0].observed_notional, 310.0, places=4)

    def test_hybrid_mode_prefers_trade_event_and_dedupes_snapshot_sell(self):
        wallet = "0x00000000000000000000000000000000000000a2"
        event_ts = int(datetime.now(tz=timezone.utc).timestamp()) + 5

        class _Client:
            def __init__(self) -> None:
                self.position_calls = 0
                self.trade_calls = 0

            def get_active_positions(self, _wallet: str, limit: int = 200):
                self.position_calls += 1
                if self.position_calls == 1:
                    return [
                        Position(
                            wallet=wallet,
                            token_id="token-b",
                            market_slug="fed-cut-rates-in-june",
                            outcome="YES",
                            avg_price=0.55,
                            size=900.0,
                            notional=495.0,
                            timestamp=1,
                        )
                    ]
                return []

            def get_user_trades(self, _wallet: str, *, limit: int = 1000, offset: int = 0, **_kwargs):
                self.trade_calls += 1
                if self.trade_calls == 1 or offset > 0:
                    return []
                return [
                    TradeFill(
                        wallet=wallet,
                        side="SELL",
                        token_id="token-b",
                        condition_id="condition-b",
                        market_slug="fed-cut-rates-in-june",
                        outcome="YES",
                        price=0.54,
                        size=500.0,
                        timestamp=event_ts,
                        tx_hash="0xtrade-sell",
                    )
                ]

            def get_user_activity(self, _wallet: str, *, limit: int = 500, offset: int = 0, **_kwargs):
                return []

        strategy = WalletFollowerStrategy(
            client=_Client(),
            min_increase_usd=300.0,
            min_decrease_usd=200.0,
            follow_wallet_exits=True,
            max_signals_per_cycle=5,
            min_active_positions=1,
            min_unique_markets=1,
            min_total_notional_usd=0.0,
            max_top_market_share=1.0,
            min_wallet_score=0.0,
            signal_source="hybrid",
        )
        strategy.update_wallet_activity_counts({wallet: 10})

        self.assertEqual(strategy.generate_signals([wallet]), [])

        signals = strategy.generate_signals([wallet])

        self.assertEqual(len(signals), 1)
        self.assertEqual(signals[0].side, "SELL")
        self.assertEqual(signals[0].condition_id, "condition-b")
        self.assertEqual(signals[0].position_action_label, "事件退出")
        self.assertAlmostEqual(signals[0].exit_fraction, 1.0, places=4)
        self.assertIn("via trades", signals[0].exit_reason)

    def test_warmup_primes_cursor_without_replaying_historical_trade(self):
        wallet = "0x00000000000000000000000000000000000000a3"
        now_ts = int(datetime.now(tz=timezone.utc).timestamp())
        historical_ts = now_ts - 30
        new_trade_ts = now_ts + 5

        class _Client:
            def __init__(self) -> None:
                self.position_calls = 0
                self.trade_calls = 0

            def get_active_positions(self, _wallet: str, limit: int = 200):
                self.position_calls += 1
                if self.position_calls < 3:
                    return [
                        Position(
                            wallet=wallet,
                            token_id="token-c",
                            market_slug="will-eth-close-above-6k",
                            outcome="YES",
                            avg_price=0.58,
                            size=1000.0,
                            notional=580.0,
                            timestamp=1,
                        )
                    ]
                return [
                    Position(
                        wallet=wallet,
                        token_id="token-c",
                        market_slug="will-eth-close-above-6k",
                        outcome="YES",
                        avg_price=0.60,
                        size=1500.0,
                        notional=900.0,
                        timestamp=2,
                    )
                ]

            def get_user_trades(self, _wallet: str, *, limit: int = 1000, offset: int = 0, **_kwargs):
                self.trade_calls += 1
                if offset > 0:
                    return []
                if self.trade_calls < 3:
                    return [
                        TradeFill(
                            wallet=wallet,
                            side="BUY",
                            token_id="token-c",
                            condition_id="condition-c",
                            market_slug="will-eth-close-above-6k",
                            outcome="YES",
                            price=0.59,
                            size=400.0,
                            timestamp=historical_ts,
                            tx_hash="0xtrade-old",
                        )
                    ]
                return [
                    TradeFill(
                        wallet=wallet,
                        side="BUY",
                        token_id="token-c",
                        condition_id="condition-c",
                        market_slug="will-eth-close-above-6k",
                        outcome="YES",
                        price=0.66,
                        size=500.0,
                        timestamp=new_trade_ts,
                        tx_hash="0xtrade-new",
                    )
                ]

            def get_user_activity(self, _wallet: str, *, limit: int = 500, offset: int = 0, **_kwargs):
                return []

        strategy = WalletFollowerStrategy(
            client=_Client(),
            min_increase_usd=300.0,
            min_decrease_usd=200.0,
            max_signals_per_cycle=5,
            min_active_positions=1,
            min_unique_markets=1,
            min_total_notional_usd=0.0,
            max_top_market_share=1.0,
            min_wallet_score=0.0,
            signal_source="trades",
        )
        strategy.update_wallet_activity_counts({wallet: 10})

        self.assertEqual(strategy.generate_signals([wallet]), [])
        self.assertEqual(strategy.generate_signals([wallet]), [])

        signals = strategy.generate_signals([wallet])

        self.assertEqual(len(signals), 1)
        self.assertEqual(signals[0].side, "BUY")
        self.assertEqual(signals[0].condition_id, "condition-c")
        self.assertEqual(signals[0].position_action_label, "事件加仓")
        self.assertAlmostEqual(signals[0].price_hint, 0.66, places=4)

    def test_activity_source_can_drive_buy_signal(self):
        wallet = "0x00000000000000000000000000000000000000a4"
        event_ts = int(datetime.now(tz=timezone.utc).timestamp()) + 5

        class _Client:
            def __init__(self) -> None:
                self.position_calls = 0
                self.activity_calls = 0

            def get_active_positions(self, _wallet: str, limit: int = 200):
                self.position_calls += 1
                if self.position_calls == 1:
                    return [
                        Position(
                            wallet=wallet,
                            token_id="token-d",
                            market_slug="will-sol-close-above-400",
                            outcome="YES",
                            avg_price=0.50,
                            size=1000.0,
                            notional=500.0,
                            timestamp=1,
                        )
                    ]
                return [
                    Position(
                        wallet=wallet,
                        token_id="token-d",
                        market_slug="will-sol-close-above-400",
                        outcome="YES",
                        avg_price=0.51,
                        size=1700.0,
                        notional=867.0,
                        timestamp=2,
                    )
                ]

            def get_user_activity(self, _wallet: str, *, limit: int = 500, offset: int = 0, **_kwargs):
                self.activity_calls += 1
                if self.activity_calls == 1 or offset > 0:
                    return []
                return [
                    ActivityEvent(
                        wallet=wallet,
                        activity_type="TRADE",
                        token_id="token-d",
                        condition_id="condition-d",
                        market_slug="will-sol-close-above-400",
                        outcome="YES",
                        side="BUY",
                        price=0.51,
                        size=700.0,
                        usdc_size=357.0,
                        timestamp=event_ts,
                        tx_hash="0xactivity-buy",
                    )
                ]

        strategy = WalletFollowerStrategy(
            client=_Client(),
            min_increase_usd=300.0,
            min_decrease_usd=200.0,
            max_signals_per_cycle=5,
            min_active_positions=1,
            min_unique_markets=1,
            min_total_notional_usd=0.0,
            max_top_market_share=1.0,
            min_wallet_score=0.0,
            signal_source="activity",
        )
        strategy.update_wallet_activity_counts({wallet: 10})

        self.assertEqual(strategy.generate_signals([wallet]), [])

        signals = strategy.generate_signals([wallet])

        self.assertEqual(len(signals), 1)
        self.assertEqual(signals[0].side, "BUY")
        self.assertEqual(signals[0].condition_id, "condition-d")
        self.assertEqual(signals[0].position_action_label, "事件加仓")
        self.assertAlmostEqual(signals[0].observed_notional, 357.0, places=4)

    def test_position_diff_fallback_carries_condition_id(self):
        wallet = "0x00000000000000000000000000000000000000a5"

        class _Client:
            def __init__(self) -> None:
                self.position_calls = 0

            def get_active_positions(self, _wallet: str, limit: int = 200):
                self.position_calls += 1
                if self.position_calls == 1:
                    return [
                        Position(
                            wallet=wallet,
                            token_id="token-e",
                            condition_id="condition-e",
                            market_slug="will-spx-close-above-6000",
                            outcome="YES",
                            avg_price=0.49,
                            size=1000.0,
                            notional=490.0,
                            timestamp=1,
                        )
                    ]
                return [
                    Position(
                        wallet=wallet,
                        token_id="token-e",
                        condition_id="condition-e",
                        market_slug="will-spx-close-above-6000",
                        outcome="YES",
                        avg_price=0.52,
                        size=1700.0,
                        notional=884.0,
                        timestamp=2,
                    )
                ]

            def get_user_trades(self, _wallet: str, *, limit: int = 1000, offset: int = 0, **_kwargs):
                return []

            def get_user_activity(self, _wallet: str, *, limit: int = 500, offset: int = 0, **_kwargs):
                return []

        strategy = WalletFollowerStrategy(
            client=_Client(),
            min_increase_usd=300.0,
            min_decrease_usd=200.0,
            max_signals_per_cycle=5,
            min_active_positions=1,
            min_unique_markets=1,
            min_total_notional_usd=0.0,
            max_top_market_share=1.0,
            min_wallet_score=0.0,
            signal_source="positions",
        )
        strategy.update_wallet_activity_counts({wallet: 10})

        self.assertEqual(strategy.generate_signals([wallet]), [])

        signals = strategy.generate_signals([wallet])

        self.assertEqual(len(signals), 1)
        self.assertEqual(signals[0].side, "BUY")
        self.assertEqual(signals[0].condition_id, "condition-e")
        self.assertEqual(signals[0].position_action_label, "追加买入")


if __name__ == "__main__":
    unittest.main()
