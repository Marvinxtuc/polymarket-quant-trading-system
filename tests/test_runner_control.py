from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timezone

from polymarket_bot.config import Settings
from polymarket_bot.runner import Trader
from polymarket_bot.types import ExecutionResult, RiskDecision, Signal


class _DummyDataClient:
    def discover_wallet_activity(self, paths, limit):
        return {}

    def close(self):
        return None


class _DummyStrategy:
    def __init__(self, signals):
        self._signals = list(signals)

    def generate_signals(self, wallets):
        return list(self._signals)


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


def _signal(side: str = "BUY") -> Signal:
    return Signal(
        wallet="0x1111111111111111111111111111111111111111",
        market_slug="demo-market",
        token_id="token-demo",
        outcome="YES",
        side=side,  # type: ignore[arg-type]
        confidence=0.8,
        price_hint=0.6,
        observed_size=10.0,
        observed_notional=100.0,
        timestamp=datetime.now(tz=timezone.utc),
    )


class TraderControlTests(unittest.TestCase):
    def _make_settings(self, control_path: str) -> Settings:
        return Settings(
            wallet_discovery_enabled=False,
            watch_wallets="0x1111111111111111111111111111111111111111",
            control_path=control_path,
            max_signals_per_cycle=1,
            poll_interval_seconds=60,
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


if __name__ == "__main__":
    unittest.main()
