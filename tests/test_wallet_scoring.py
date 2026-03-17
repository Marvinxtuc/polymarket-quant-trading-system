from __future__ import annotations

import unittest

from polymarket_bot.clients.data_api import ClosedPosition, Position, ResolvedMarket
from polymarket_bot.strategies.wallet_follower import WalletFollowerStrategy
from polymarket_bot.wallet_scoring import (
    RealizedWalletMetrics,
    SmartWalletScorer,
    build_realized_wallet_metrics,
)


class SmartWalletScorerTests(unittest.TestCase):
    def test_high_quality_wallet_scores_as_core(self):
        scorer = SmartWalletScorer()

        score = scorer.score_wallet(
            total_notional_usd=9_500.0,
            active_positions=8,
            unique_markets=9,
            top_market_share=0.28,
            recent_activity_events=14,
        )

        self.assertGreaterEqual(score.score, 80.0)
        self.assertEqual(score.tier, "CORE")
        self.assertTrue(score.activity_known)

    def test_unknown_activity_uses_neutral_baseline(self):
        scorer = SmartWalletScorer()

        score = scorer.score_wallet(
            total_notional_usd=1_200.0,
            active_positions=3,
            unique_markets=3,
            top_market_share=0.55,
            recent_activity_events=None,
        )

        self.assertFalse(score.activity_known)
        self.assertGreater(score.components["activity"], 0.0)
        self.assertIn("recent activity unknown", score.summary)

    def test_strategy_filters_wallets_below_min_score(self):
        class _Client:
            def get_active_positions(self, wallet: str, limit: int = 200):
                if wallet.endswith("1"):
                    return [
                        Position(wallet=wallet, token_id="a", market_slug="m1", outcome="YES", avg_price=0.6, size=600.0, notional=360.0, timestamp=1),
                        Position(wallet=wallet, token_id="b", market_slug="m2", outcome="NO", avg_price=0.6, size=400.0, notional=240.0, timestamp=1),
                    ]
                return [
                    Position(wallet=wallet, token_id="c", market_slug="m3", outcome="YES", avg_price=0.6, size=2500.0, notional=1500.0, timestamp=1),
                    Position(wallet=wallet, token_id="d", market_slug="m4", outcome="NO", avg_price=0.6, size=2500.0, notional=1500.0, timestamp=1),
                    Position(wallet=wallet, token_id="e", market_slug="m5", outcome="YES", avg_price=0.6, size=1200.0, notional=720.0, timestamp=1),
                    Position(wallet=wallet, token_id="f", market_slug="m6", outcome="NO", avg_price=0.6, size=1200.0, notional=720.0, timestamp=1),
                ]

        strategy = WalletFollowerStrategy(
            client=_Client(),
            min_increase_usd=300.0,
            max_signals_per_cycle=5,
            min_active_positions=2,
            min_unique_markets=2,
            min_total_notional_usd=500.0,
            max_top_market_share=0.85,
            min_wallet_score=60.0,
        )
        strategy.update_wallet_activity_counts(
            {
                "0x0000000000000000000000000000000000000001": 2,
                "0x0000000000000000000000000000000000000002": 12,
            }
        )

        selected = strategy._screen_wallets(
            [
                "0x0000000000000000000000000000000000000001",
                "0x0000000000000000000000000000000000000002",
            ]
        )

        self.assertNotIn("0x0000000000000000000000000000000000000001", selected)
        self.assertIn("0x0000000000000000000000000000000000000002", selected)
        self.assertFalse(strategy.latest_wallet_metrics()["0x0000000000000000000000000000000000000001"]["trading_enabled"])
        self.assertTrue(strategy.latest_wallet_metrics()["0x0000000000000000000000000000000000000002"]["trading_enabled"])

    def test_realized_metrics_capture_roi_and_resolved_accuracy(self):
        closed_positions = [
            ClosedPosition(
                wallet="0x1111111111111111111111111111111111111111",
                token_id="token-a",
                condition_id="condition-a",
                market_slug="market-a",
                outcome="YES",
                avg_price=0.52,
                total_bought=100.0,
                realized_pnl=25.0,
                timestamp=1,
                end_date="2026-03-01T00:00:00Z",
            ),
            ClosedPosition(
                wallet="0x1111111111111111111111111111111111111111",
                token_id="token-b",
                condition_id="condition-b",
                market_slug="market-b",
                outcome="NO",
                avg_price=0.47,
                total_bought=100.0,
                realized_pnl=-5.0,
                timestamp=2,
                end_date="2026-03-02T00:00:00Z",
            ),
        ]
        resolution_map = {
            "condition-a": ResolvedMarket(
                condition_id="condition-a",
                winner_token_id="token-a",
                winner_outcome="YES",
                closed=True,
            ),
            "condition-b": ResolvedMarket(
                condition_id="condition-b",
                winner_token_id="token-c",
                winner_outcome="YES",
                closed=True,
            ),
        }

        metrics = build_realized_wallet_metrics(closed_positions, resolution_map)

        self.assertEqual(metrics.closed_positions, 2)
        self.assertEqual(metrics.wins, 1)
        self.assertAlmostEqual(metrics.realized_pnl, 20.0, places=2)
        self.assertAlmostEqual(metrics.roi, 0.1, places=4)
        self.assertAlmostEqual(metrics.win_rate, 0.5, places=4)
        self.assertEqual(metrics.resolved_markets, 2)
        self.assertEqual(metrics.resolved_wins, 1)
        self.assertAlmostEqual(metrics.resolved_win_rate, 0.5, places=4)
        self.assertAlmostEqual(metrics.profit_factor, 5.0, places=4)

    def test_strong_realized_history_lifts_score_above_proxy_baseline(self):
        scorer = SmartWalletScorer()
        realized_metrics = RealizedWalletMetrics(
            closed_positions=18,
            wins=13,
            resolved_markets=12,
            resolved_wins=9,
            total_bought=2_400.0,
            realized_pnl=360.0,
            gross_profit=480.0,
            gross_loss=120.0,
            win_rate=13 / 18,
            resolved_win_rate=0.75,
            roi=0.15,
            profit_factor=4.0,
        )

        proxy_only = scorer.score_wallet(
            total_notional_usd=1_800.0,
            active_positions=3,
            unique_markets=3,
            top_market_share=0.55,
            recent_activity_events=2,
        )
        score_with_history = scorer.score_wallet(
            total_notional_usd=1_800.0,
            active_positions=3,
            unique_markets=3,
            top_market_share=0.55,
            recent_activity_events=2,
            realized_metrics=realized_metrics,
        )

        self.assertGreater(score_with_history.score, proxy_only.score)
        self.assertGreaterEqual(score_with_history.score, 65.0)
        self.assertIn("roi", score_with_history.summary)

    def test_strategy_can_admit_wallet_on_realized_history_strength(self):
        class _Client:
            def get_active_positions(self, wallet: str, limit: int = 200):
                return [
                    Position(wallet=wallet, token_id="a", market_slug="m1", outcome="YES", avg_price=0.58, size=700.0, notional=406.0, timestamp=1),
                    Position(wallet=wallet, token_id="b", market_slug="m2", outcome="NO", avg_price=0.52, size=650.0, notional=338.0, timestamp=1),
                ]

        strategy = WalletFollowerStrategy(
            client=_Client(),
            min_increase_usd=300.0,
            max_signals_per_cycle=5,
            min_active_positions=2,
            min_unique_markets=2,
            min_total_notional_usd=500.0,
            max_top_market_share=0.85,
            min_wallet_score=60.0,
        )
        strategy.update_wallet_activity_counts(
            {"0x0000000000000000000000000000000000000003": 1}
        )
        strategy.update_wallet_realized_metrics(
            {
                "0x0000000000000000000000000000000000000003": RealizedWalletMetrics(
                    closed_positions=18,
                    wins=13,
                    resolved_markets=10,
                    resolved_wins=8,
                    total_bought=2500.0,
                    realized_pnl=350.0,
                    gross_profit=470.0,
                    gross_loss=120.0,
                    win_rate=13 / 18,
                    resolved_win_rate=0.8,
                    roi=0.14,
                    profit_factor=3.92,
                )
            },
            refreshed_ts={"0x0000000000000000000000000000000000000003": 123},
        )

        selected = strategy._screen_wallets(
            ["0x0000000000000000000000000000000000000003"]
        )

        self.assertIn("0x0000000000000000000000000000000000000003", selected)
        metrics = strategy.latest_wallet_metrics()["0x0000000000000000000000000000000000000003"]
        self.assertTrue(metrics["trading_enabled"])
        self.assertGreaterEqual(float(metrics["wallet_score"]), 60.0)
        self.assertAlmostEqual(float(metrics["roi"]), 0.14, places=4)
        self.assertAlmostEqual(float(metrics["resolved_win_rate"]), 0.8, places=4)
        self.assertEqual(int(metrics["history_refresh_ts"]), 123)

    def test_strategy_emits_sell_signal_when_wallet_trims_position(self):
        wallet = "0x0000000000000000000000000000000000000009"

        class _Client:
            def __init__(self):
                self.calls = 0

            def get_active_positions(self, wallet: str, limit: int = 200):
                self.calls += 1
                if self.calls == 1:
                    return [
                        Position(
                            wallet=wallet,
                            token_id="token-a",
                            market_slug="will-btc-close-above-100k",
                            outcome="YES",
                            avg_price=0.6,
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
                        avg_price=0.58,
                        size=500.0,
                        notional=290.0,
                        timestamp=2,
                    )
                ]

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
        )
        strategy.update_wallet_activity_counts({wallet: 8})

        warmup = strategy.generate_signals([wallet])
        self.assertEqual(warmup, [])

        signals = strategy.generate_signals([wallet])
        self.assertEqual(len(signals), 1)
        self.assertEqual(signals[0].side, "SELL")
        self.assertAlmostEqual(signals[0].exit_fraction, 310.0 / 600.0, places=4)
        self.assertIn("trimmed", signals[0].exit_reason)

    def test_strategy_emits_full_exit_signal_when_wallet_closes_position(self):
        wallet = "0x0000000000000000000000000000000000000010"

        class _Client:
            def __init__(self):
                self.calls = 0

            def get_active_positions(self, wallet: str, limit: int = 200):
                self.calls += 1
                if self.calls == 1:
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
        )
        strategy.update_wallet_activity_counts({wallet: 8})

        self.assertEqual(strategy.generate_signals([wallet]), [])
        signals = strategy.generate_signals([wallet])

        self.assertEqual(len(signals), 1)
        self.assertEqual(signals[0].side, "SELL")
        self.assertAlmostEqual(signals[0].exit_fraction, 1.0, places=4)
        self.assertEqual(signals[0].exit_reason, "source wallet fully exited")

    def test_strategy_emits_resonance_exit_signal_when_two_strong_wallets_trim_same_token(self):
        wallets = [
            "0x0000000000000000000000000000000000000011",
            "0x0000000000000000000000000000000000000012",
        ]

        class _Client:
            def __init__(self):
                self.calls = 0

            def get_active_positions(self, wallet: str, limit: int = 200):
                cycle = self.calls // 2
                self.calls += 1
                if cycle == 0:
                    return [
                        Position(
                            wallet=wallet,
                            token_id="token-r",
                            market_slug="will-btc-close-above-100k",
                            outcome="YES",
                            avg_price=0.6,
                            size=1000.0,
                            notional=600.0,
                            timestamp=1,
                        )
                    ]
                return [
                    Position(
                        wallet=wallet,
                        token_id="token-r",
                        market_slug="will-btc-close-above-100k",
                        outcome="YES",
                        avg_price=0.58,
                        size=600.0,
                        notional=340.0,
                        timestamp=2,
                    )
                ]

        strategy = WalletFollowerStrategy(
            client=_Client(),
            min_increase_usd=300.0,
            min_decrease_usd=200.0,
            follow_wallet_exits=True,
            resonance_exit_enabled=True,
            resonance_min_wallets=2,
            resonance_min_wallet_score=65.0,
            resonance_trim_fraction=0.35,
            resonance_core_exit_fraction=0.6,
            max_signals_per_cycle=10,
            min_active_positions=1,
            min_unique_markets=1,
            min_total_notional_usd=0.0,
            max_top_market_share=1.0,
            min_wallet_score=0.0,
        )
        strategy.update_wallet_activity_counts({wallets[0]: 10, wallets[1]: 9})
        strategy.update_wallet_realized_metrics(
            {
                wallets[0]: RealizedWalletMetrics(
                    closed_positions=18,
                    wins=13,
                    resolved_markets=10,
                    resolved_wins=8,
                    total_bought=2500.0,
                    realized_pnl=350.0,
                    gross_profit=470.0,
                    gross_loss=120.0,
                    win_rate=13 / 18,
                    resolved_win_rate=0.8,
                    roi=0.14,
                    profit_factor=3.92,
                ),
                wallets[1]: RealizedWalletMetrics(
                    closed_positions=16,
                    wins=11,
                    resolved_markets=9,
                    resolved_wins=7,
                    total_bought=2200.0,
                    realized_pnl=260.0,
                    gross_profit=390.0,
                    gross_loss=130.0,
                    win_rate=11 / 16,
                    resolved_win_rate=7 / 9,
                    roi=0.1182,
                    profit_factor=3.0,
                ),
            }
        )

        self.assertEqual(strategy.generate_signals(wallets), [])
        signals = strategy.generate_signals(wallets)

        resonance = [signal for signal in signals if signal.cross_wallet_exit]
        self.assertEqual(len(resonance), 1)
        self.assertEqual(resonance[0].side, "SELL")
        self.assertEqual(resonance[0].exit_wallet_count, 2)
        self.assertAlmostEqual(resonance[0].exit_fraction, 0.35, places=4)
        self.assertIn("multi-wallet exit resonance", resonance[0].exit_reason)


if __name__ == "__main__":
    unittest.main()
