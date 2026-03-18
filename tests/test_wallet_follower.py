from __future__ import annotations

import unittest
from datetime import datetime, timezone

from polymarket_bot.clients.data_api import ActivityEvent, Position, TradeFill
from polymarket_bot.strategies.wallet_follower import WalletFollowerStrategy


class WalletFollowerEventSignalTests(unittest.TestCase):
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
