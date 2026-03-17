from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timezone

from polymarket_bot.config import Settings
from polymarket_bot.runner import Trader
from polymarket_bot.types import ExecutionResult, RiskDecision, Signal
from polymarket_bot.wallet_scoring import RealizedWalletMetrics


class _DummyDataClient:
    def discover_wallet_activity(self, paths, limit):
        return {}

    def close(self):
        return None


class _DummyStrategy:
    def __init__(self, signals):
        self._signals = list(signals)
        self.selection_context = {}

    def generate_signals(self, wallets):
        return list(self._signals)

    def update_wallet_selection_context(self, context):
        self.selection_context = dict(context)


class _DummyRisk:
    def evaluate(self, signal, state):
        return RiskDecision(True, "ok", 50.0)


class _DummyBroker:
    def __init__(self):
        self.calls = []

    def execute(self, signal, notional_usd):
        self.calls.append((signal, notional_usd))
        return ExecutionResult(
            ok=True,
            broker_order_id="paper-test",
            message="ok",
            filled_notional=notional_usd,
            filled_price=max(0.01, signal.price_hint),
        )


class _FakeHistoryStore:
    def __init__(self, metrics=None, topic_profiles=None):
        self.metrics = dict(metrics or {})
        self.topic_profiles = dict(topic_profiles or {})

    def sync_wallets(self, wallets, *, max_wallets=None):
        selected = []
        limit = len(wallets) if max_wallets is None else max_wallets
        for wallet in wallets:
            key = str(wallet).strip().lower()
            if key and key not in selected:
                selected.append(key)
            if len(selected) >= limit:
                break
        metrics = {wallet: self.metrics[wallet] for wallet in selected if wallet in self.metrics}
        refreshed = {wallet: 1700000000 for wallet in selected if wallet in self.metrics}
        return metrics, refreshed, {}, {wallet: list(self.topic_profiles.get(wallet, [])) for wallet in selected if wallet in self.topic_profiles}

    def peek_wallets(self, wallets):
        selected = []
        for wallet in wallets:
            key = str(wallet).strip().lower()
            if key and key not in selected:
                selected.append(key)
        metrics = {wallet: self.metrics[wallet] for wallet in selected if wallet in self.metrics}
        refreshed = {wallet: 1700000000 for wallet in selected if wallet in self.metrics}
        return metrics, refreshed, {}, {wallet: list(self.topic_profiles.get(wallet, [])) for wallet in selected if wallet in self.topic_profiles}


def _signal(
    side: str = "BUY",
    wallet_score: float = 80.0,
    wallet_tier: str = "CORE",
    **kwargs: object,
) -> Signal:
    return Signal(
        signal_id="",
        trace_id="",
        wallet=str(kwargs.get("wallet", "0x1111111111111111111111111111111111111111")),
        market_slug=str(kwargs.get("market_slug", "demo-market")),
        token_id="token-demo",
        outcome="YES",
        side=side,  # type: ignore[arg-type]
        confidence=0.8,
        price_hint=0.6,
        observed_size=float(kwargs.get("observed_size", 10.0)),
        observed_notional=float(kwargs.get("observed_notional", 100.0)),
        timestamp=datetime.now(tz=timezone.utc),
        wallet_score=wallet_score,
        wallet_tier=wallet_tier,
        topic_key=str(kwargs.get("topic_key", "")),
        topic_label=str(kwargs.get("topic_label", "")),
        topic_sample_count=int(kwargs.get("topic_sample_count", 0)),
        topic_win_rate=float(kwargs.get("topic_win_rate", 0.0)),
        topic_roi=float(kwargs.get("topic_roi", 0.0)),
        topic_resolved_win_rate=float(kwargs.get("topic_resolved_win_rate", 0.0)),
        topic_score_summary=str(kwargs.get("topic_score_summary", "")),
        exit_fraction=float(kwargs.get("exit_fraction", 0.0)),
        exit_reason=str(kwargs.get("exit_reason", "")),
        cross_wallet_exit=bool(kwargs.get("cross_wallet_exit", False)),
        exit_wallet_count=int(kwargs.get("exit_wallet_count", 0)),
    )


class TraderControlTests(unittest.TestCase):
    def _make_settings(self, control_path: str, **kwargs: object) -> Settings:
        max_signals_per_cycle = int(kwargs.get("max_signals_per_cycle", 1))
        runtime_state_path = kwargs.get("runtime_state_path")
        if runtime_state_path is None:
            runtime_state_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
            runtime_state_file.write("{}")
            runtime_state_file.flush()
            runtime_state_file.close()
            runtime_state_path = runtime_state_file.name

        return Settings(
            _env_file=None,
            wallet_discovery_enabled=bool(kwargs.get("wallet_discovery_enabled", False)),
            wallet_discovery_mode=str(kwargs.get("wallet_discovery_mode", "union")),
            watch_wallets=str(kwargs.get("watch_wallets", "0x1111111111111111111111111111111111111111")),
            wallet_discovery_top_n=int(kwargs.get("wallet_discovery_top_n", 50)),
            wallet_discovery_min_events=int(kwargs.get("wallet_discovery_min_events", 2)),
            wallet_discovery_refresh_seconds=int(kwargs.get("wallet_discovery_refresh_seconds", 900)),
            wallet_discovery_quality_bias_enabled=bool(kwargs.get("wallet_discovery_quality_bias_enabled", True)),
            wallet_discovery_quality_top_n=int(kwargs.get("wallet_discovery_quality_top_n", 16)),
            wallet_discovery_history_bonus=float(kwargs.get("wallet_discovery_history_bonus", 0.75)),
            wallet_discovery_topic_bonus=float(kwargs.get("wallet_discovery_topic_bonus", 0.5)),
            control_path=control_path,
            runtime_state_path=str(runtime_state_path),
            max_signals_per_cycle=max_signals_per_cycle,
            poll_interval_seconds=60,
            bankroll_usd=float(kwargs.get("bankroll_usd", 5000.0)),
            order_dedup_ttl_seconds=int(kwargs.get("order_dedup_ttl_seconds", 120)),
            runtime_reconcile_interval_seconds=int(kwargs.get("runtime_reconcile_interval_seconds", 180)),
            min_wallet_score=float(kwargs.get("min_wallet_score", 50.0)),
            wallet_score_watch_multiplier=float(kwargs.get("wallet_score_watch_multiplier", 0.4)),
            wallet_score_trade_multiplier=float(kwargs.get("wallet_score_trade_multiplier", 0.75)),
            wallet_score_core_multiplier=float(kwargs.get("wallet_score_core_multiplier", 1.0)),
            topic_bias_enabled=bool(kwargs.get("topic_bias_enabled", True)),
            topic_min_samples=int(kwargs.get("topic_min_samples", 3)),
            topic_positive_roi=float(kwargs.get("topic_positive_roi", 0.08)),
            topic_positive_win_rate=float(kwargs.get("topic_positive_win_rate", 0.6)),
            topic_negative_roi=float(kwargs.get("topic_negative_roi", -0.02)),
            topic_negative_win_rate=float(kwargs.get("topic_negative_win_rate", 0.45)),
            topic_boost_multiplier=float(kwargs.get("topic_boost_multiplier", 1.1)),
            topic_penalty_multiplier=float(kwargs.get("topic_penalty_multiplier", 0.9)),
            wallet_exit_follow_enabled=bool(kwargs.get("wallet_exit_follow_enabled", True)),
            min_wallet_decrease_usd=float(kwargs.get("min_wallet_decrease_usd", 200.0)),
            resonance_exit_enabled=bool(kwargs.get("resonance_exit_enabled", True)),
            resonance_min_wallets=int(kwargs.get("resonance_min_wallets", 2)),
            resonance_min_wallet_score=float(kwargs.get("resonance_min_wallet_score", 65.0)),
            resonance_trim_fraction=float(kwargs.get("resonance_trim_fraction", 0.35)),
            resonance_core_exit_fraction=float(kwargs.get("resonance_core_exit_fraction", 0.6)),
        )

    def test_pause_opening_blocks_buy_signal(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "pause_opening": True,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 1,
                },
                f,
            )
            control_path = f.name

        broker = _DummyBroker()
        trader = Trader(
            settings=self._make_settings(control_path),
            data_client=_DummyDataClient(),
            strategy=_DummyStrategy([_signal("BUY")]),
            risk=_DummyRisk(),
            broker=broker,
        )

        trader.step()
        self.assertEqual(len(broker.calls), 0)

    def test_emergency_stop_sells_existing_positions(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": True,
                    "updated_ts": 2,
                },
                f,
            )
            control_path = f.name

        broker = _DummyBroker()
        trader = Trader(
            settings=self._make_settings(control_path),
            data_client=_DummyDataClient(),
            strategy=_DummyStrategy([]),
            risk=_DummyRisk(),
            broker=broker,
        )
        trader.positions_book["token-demo"] = {
            "token_id": "token-demo",
            "market_slug": "demo-market",
            "outcome": "YES",
            "quantity": 100.0,
            "price": 0.6,
            "notional": 60.0,
            "opened_ts": 0,
            "last_buy_ts": 0,
        }
        trader.state.open_positions = 1

        trader.step()

        self.assertEqual(len(broker.calls), 1)
        self.assertEqual(broker.calls[0][0].side, "SELL")
        self.assertEqual(trader.state.open_positions, 0)
        self.assertNotIn("token-demo", trader.positions_book)

    def test_buy_signal_skipped_when_budget_exhausted(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 3,
                },
                f,
            )
            control_path = f.name

        broker = _DummyBroker()
        trader = Trader(
            settings=self._make_settings(control_path, bankroll_usd=100.0),
            data_client=_DummyDataClient(),
            strategy=_DummyStrategy([_signal("BUY")]),
            risk=_DummyRisk(),
            broker=broker,
        )
        trader.positions_book["token-demo"] = {
            "token_id": "token-demo",
            "market_slug": "demo-market",
            "outcome": "YES",
            "quantity": 6.0,
            "price": 0.6,
            "notional": 96.0,
            "opened_ts": 0,
            "last_buy_ts": 0,
        }
        trader.state.open_positions = 1

        trader.step()

        self.assertEqual(len(broker.calls), 0)

    def test_duplicate_buy_signal_is_debounced(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 4,
                },
                f,
            )
            control_path = f.name

        broker = _DummyBroker()
        trader = Trader(
            settings=self._make_settings(control_path, max_signals_per_cycle=2, order_dedup_ttl_seconds=120),
            data_client=_DummyDataClient(),
            strategy=_DummyStrategy([_signal("BUY"), _signal("BUY")]),
            risk=_DummyRisk(),
            broker=broker,
        )
        trader.step()
        self.assertEqual(len(broker.calls), 1)

    def test_wallet_score_trade_tier_reduces_order_size(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 5,
                },
                f,
            )
            control_path = f.name

        broker = _DummyBroker()
        trader = Trader(
            settings=self._make_settings(control_path, wallet_score_trade_multiplier=0.75),
            data_client=_DummyDataClient(),
            strategy=_DummyStrategy([_signal("BUY", wallet_score=72.0, wallet_tier="TRADE")]),
            risk=_DummyRisk(),
            broker=broker,
        )

        trader.step()
        self.assertEqual(len(broker.calls), 1)
        self.assertEqual(broker.calls[0][1], 37.5)
        self.assertTrue(trader.last_signals[0].signal_id.startswith("sig-"))
        self.assertTrue(trader.last_signals[0].trace_id.startswith("trc-"))
        self.assertEqual(trader.recent_signal_cycles[0]["candidates"][0]["final_status"], "filled")
        self.assertTrue(str(trader.recent_orders[0]["trace_id"]).startswith("trc-"))

    def test_wallet_score_below_min_is_skipped(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 6,
                },
                f,
            )
            control_path = f.name

        broker = _DummyBroker()
        trader = Trader(
            settings=self._make_settings(control_path, min_wallet_score=50.0),
            data_client=_DummyDataClient(),
            strategy=_DummyStrategy([_signal("BUY", wallet_score=45.0, wallet_tier="LOW")]),
            risk=_DummyRisk(),
            broker=broker,
        )

        trader.step()
        self.assertEqual(len(broker.calls), 0)

    def test_topic_profile_boost_increases_order_size(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 7,
                },
                f,
            )
            control_path = f.name

        broker = _DummyBroker()
        trader = Trader(
            settings=self._make_settings(control_path, topic_boost_multiplier=1.1),
            data_client=_DummyDataClient(),
            strategy=_DummyStrategy([
                _signal(
                    "BUY",
                    wallet_score=82.0,
                    wallet_tier="CORE",
                    market_slug="will-btc-close-above-100k",
                    topic_key="crypto",
                    topic_label="加密",
                    topic_sample_count=6,
                    topic_win_rate=0.72,
                    topic_roi=0.16,
                    topic_score_summary="加密 | 6 samples | roi +16% | win 72%",
                )
            ]),
            risk=_DummyRisk(),
            broker=broker,
        )

        trader.step()
        self.assertEqual(len(broker.calls), 1)
        self.assertAlmostEqual(broker.calls[0][1], 55.0, places=4)
        self.assertIn("加密 boost x1.10", trader.recent_orders[0]["reason"])

    def test_topic_profile_penalty_reduces_order_size(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 8,
                },
                f,
            )
            control_path = f.name

        broker = _DummyBroker()
        trader = Trader(
            settings=self._make_settings(control_path, topic_penalty_multiplier=0.9),
            data_client=_DummyDataClient(),
            strategy=_DummyStrategy([
                _signal(
                    "BUY",
                    wallet_score=82.0,
                    wallet_tier="CORE",
                    market_slug="fed-cut-rates-in-june",
                    topic_key="macro",
                    topic_label="宏观",
                    topic_sample_count=5,
                    topic_win_rate=0.4,
                    topic_roi=-0.05,
                    topic_score_summary="宏观 | 5 samples | roi -5% | win 40%",
                )
            ]),
            risk=_DummyRisk(),
            broker=broker,
        )

        trader.step()
        self.assertEqual(len(broker.calls), 1)
        self.assertAlmostEqual(broker.calls[0][1], 45.0, places=4)
        self.assertIn("宏观 trim x0.90", trader.recent_orders[0]["reason"])

    def test_sell_signal_reduces_existing_position_by_exit_fraction(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 8,
                },
                f,
            )
            control_path = f.name

        broker = _DummyBroker()
        trader = Trader(
            settings=self._make_settings(control_path),
            data_client=_DummyDataClient(),
            strategy=_DummyStrategy([
                _signal(
                    "SELL",
                    wallet="0x1111111111111111111111111111111111111111",
                    wallet_score=82.0,
                    wallet_tier="CORE",
                    observed_notional=80.0,
                    exit_fraction=0.5,
                    exit_reason="source wallet trimmed 50% | delta $400",
                )
            ]),
            risk=_DummyRisk(),
            broker=broker,
        )
        trader.positions_book["token-demo"] = {
            "token_id": "token-demo",
            "market_slug": "demo-market",
            "outcome": "YES",
            "quantity": 100.0,
            "price": 0.6,
            "notional": 60.0,
            "opened_ts": 0,
            "last_buy_ts": 0,
            "entry_wallet": "0x1111111111111111111111111111111111111111",
            "entry_wallet_score": 82.0,
            "entry_wallet_tier": "CORE",
        }
        trader.state.open_positions = 1

        trader.step()

        self.assertEqual(len(broker.calls), 1)
        self.assertAlmostEqual(broker.calls[0][1], 30.0, places=4)
        self.assertAlmostEqual(trader.positions_book["token-demo"]["notional"], 30.0, places=4)
        self.assertIn("source wallet trimmed 50%", trader.recent_orders[0]["reason"])

    def test_sell_signal_is_ignored_when_entry_wallet_differs(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 9,
                },
                f,
            )
            control_path = f.name

        broker = _DummyBroker()
        trader = Trader(
            settings=self._make_settings(control_path),
            data_client=_DummyDataClient(),
            strategy=_DummyStrategy([
                _signal(
                    "SELL",
                    wallet="0x1111111111111111111111111111111111111111",
                    observed_notional=80.0,
                    exit_fraction=1.0,
                    exit_reason="source wallet fully exited",
                )
            ]),
            risk=_DummyRisk(),
            broker=broker,
        )
        trader.positions_book["token-demo"] = {
            "token_id": "token-demo",
            "market_slug": "demo-market",
            "outcome": "YES",
            "quantity": 100.0,
            "price": 0.6,
            "notional": 60.0,
            "opened_ts": 0,
            "last_buy_ts": 0,
            "entry_wallet": "0x9999999999999999999999999999999999999999",
            "entry_wallet_score": 82.0,
            "entry_wallet_tier": "CORE",
        }
        trader.state.open_positions = 1

        trader.step()

        self.assertEqual(len(broker.calls), 0)
        self.assertAlmostEqual(trader.positions_book["token-demo"]["notional"], 60.0, places=4)

    def test_cross_wallet_sell_signal_can_trim_position_once(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 10,
                },
                f,
            )
            control_path = f.name

        broker = _DummyBroker()
        trader = Trader(
            settings=self._make_settings(control_path),
            data_client=_DummyDataClient(),
            strategy=_DummyStrategy([
                _signal(
                    "SELL",
                    wallet="wallet-resonance",
                    observed_notional=120.0,
                    exit_fraction=0.35,
                    exit_reason="multi-wallet exit resonance | 2 wallets trimming",
                    cross_wallet_exit=True,
                    exit_wallet_count=2,
                ),
                _signal(
                    "SELL",
                    wallet="wallet-resonance",
                    observed_notional=120.0,
                    exit_fraction=0.6,
                    exit_reason="multi-wallet exit resonance | 2 wallets | 1 CORE full exit",
                    cross_wallet_exit=True,
                    exit_wallet_count=2,
                ),
            ]),
            risk=_DummyRisk(),
            broker=broker,
        )
        trader.positions_book["token-demo"] = {
            "token_id": "token-demo",
            "market_slug": "demo-market",
            "outcome": "YES",
            "quantity": 100.0,
            "price": 0.6,
            "notional": 60.0,
            "opened_ts": 0,
            "last_buy_ts": 0,
            "entry_wallet": "0x9999999999999999999999999999999999999999",
            "entry_wallet_score": 82.0,
            "entry_wallet_tier": "CORE",
        }
        trader.state.open_positions = 1

        trader.step()

        self.assertEqual(len(broker.calls), 1)
        self.assertAlmostEqual(broker.calls[0][1], 21.0, places=4)
        self.assertAlmostEqual(trader.positions_book["token-demo"]["notional"], 39.0, places=4)
        self.assertIn("multi-wallet exit resonance", trader.recent_orders[0]["reason"])

    def test_wallet_discovery_quality_bias_promotes_stronger_wallet(self):
        wallet_a = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        wallet_b = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 9,
                },
                f,
            )
            control_path = f.name

        metrics = {
            wallet_b: RealizedWalletMetrics(
                closed_positions=12,
                wins=8,
                resolved_markets=4,
                resolved_wins=3,
                total_bought=1000.0,
                realized_pnl=160.0,
                gross_profit=240.0,
                gross_loss=80.0,
                win_rate=0.6667,
                resolved_win_rate=0.75,
                roi=0.16,
                profit_factor=3.0,
            )
        }
        topic_profiles = {
            wallet_b: [
                {
                    "key": "crypto",
                    "label": "加密",
                    "sample_count": 6,
                    "win_rate": 0.72,
                    "roi": 0.18,
                    "resolved_markets": 3,
                    "resolved_win_rate": 0.67,
                }
            ]
        }
        data_client = _DummyDataClient()
        data_client.discover_wallet_activity = lambda paths, limit: {wallet_a: 5, wallet_b: 4}
        strategy = _DummyStrategy([])
        trader = Trader(
            settings=self._make_settings(
                control_path,
                watch_wallets="",
                wallet_discovery_enabled=True,
                wallet_discovery_top_n=5,
                wallet_discovery_min_events=1,
                wallet_discovery_quality_top_n=2,
                wallet_discovery_history_bonus=0.75,
                wallet_discovery_topic_bonus=0.5,
            ),
            data_client=data_client,
            strategy=strategy,
            risk=_DummyRisk(),
            broker=_DummyBroker(),
        )
        trader._wallet_history_store = _FakeHistoryStore(metrics=metrics, topic_profiles=topic_profiles)

        wallets = trader._resolve_wallets()

        self.assertEqual(wallets[0], wallet_b)
        self.assertEqual(strategy.selection_context[wallet_b]["discovery_priority_rank"], 1)
        self.assertIn("hist +", strategy.selection_context[wallet_b]["discovery_priority_reason"])
        self.assertIn("加密 +", strategy.selection_context[wallet_b]["discovery_priority_reason"])


if __name__ == "__main__":
    unittest.main()
