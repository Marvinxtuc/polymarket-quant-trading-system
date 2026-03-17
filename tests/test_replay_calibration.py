from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from polymarket_bot.config import Settings
from polymarket_bot.replay_calibration import (
    ReplayScenario,
    evaluate_replay_matrix,
    evaluate_replay_scenario,
    load_replay_samples,
    summarize_wallet_pools,
)


def _write_events(rows: list[dict[str, object]]) -> Path:
    handle = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
    for row in rows:
        handle.write(json.dumps(row, ensure_ascii=False))
        handle.write("\n")
    handle.flush()
    handle.close()
    return Path(handle.name)


def _write_runtime_state(cycles: list[dict[str, object]]) -> Path:
    handle = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
    json.dump({"signal_cycles": cycles}, handle, ensure_ascii=False)
    handle.flush()
    handle.close()
    return Path(handle.name)


class ReplayCalibrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.settings = Settings(_env_file=None)

    def test_entry_replay_applies_score_and_topic_multipliers(self) -> None:
        events_path = _write_events(
            [
                {
                    "type": "order_filled",
                    "ts": 1700000000,
                    "wallet": "0xaaa",
                    "market_slug": "btc-above-100k",
                    "token_id": "token-a",
                    "side": "BUY",
                    "decision_max_notional": 100.0,
                    "wallet_score": 82.0,
                    "wallet_tier": "CORE",
                    "topic_label": "加密",
                    "topic_sample_count": 6,
                    "topic_win_rate": 0.72,
                    "topic_roi": 0.16,
                }
            ]
        )
        samples = load_replay_samples(events_path)

        boosted = evaluate_replay_scenario(samples, ReplayScenario.from_settings(self.settings, name="boosted"))
        neutral = evaluate_replay_scenario(
            samples,
            ReplayScenario.from_mapping(
                {
                    "name": "neutral",
                    "topic_boost_multiplier": 1.0,
                    "topic_penalty_multiplier": 1.0,
                },
                self.settings,
            ),
        )

        self.assertAlmostEqual(boosted["simulated_entry_notional"], 110.0, places=4)
        self.assertAlmostEqual(neutral["simulated_entry_notional"], 100.0, places=4)

    def test_exit_replay_scales_resonance_and_time_exit(self) -> None:
        samples = [
            {
                "status": "FILLED",
                "flow": "exit",
                "exit_kind": "resonance_exit",
                "filled_notional": 24.0,
                "wallet_tier": "TRADE",
                "exit_fraction": 0.35,
                "hold_minutes": 45,
                "topic_label": "加密",
            },
            {
                "status": "FILLED",
                "flow": "exit",
                "exit_kind": "time_exit",
                "filled_notional": 20.0,
                "exit_fraction": 0.4,
                "hold_minutes": 10,
                "topic_label": "宏观",
            },
        ]
        faster = evaluate_replay_scenario(
            samples,
            ReplayScenario.from_mapping(
                {
                    "name": "faster",
                    "resonance_trim_fraction": 0.5,
                    "stale_position_minutes": 5,
                    "stale_position_trim_pct": 0.5,
                },
                self.settings,
            ),
        )
        slower = evaluate_replay_scenario(
            samples,
            ReplayScenario.from_mapping(
                {
                    "name": "slower",
                    "resonance_trim_fraction": 0.2,
                    "stale_position_minutes": 20,
                    "stale_position_trim_pct": 0.25,
                },
                self.settings,
            ),
        )

        self.assertGreater(faster["simulated_exit_notional"], slower["simulated_exit_notional"])
        self.assertEqual(slower["deferred_exit_count"], 1)

    def test_matrix_supports_topic_filter(self) -> None:
        samples = [
            {
                "status": "FILLED",
                "flow": "entry",
                "decision_max_notional": 100.0,
                "wallet_score": 72.0,
                "topic_label": "加密",
                "topic_sample_count": 5,
                "topic_win_rate": 0.7,
                "topic_roi": 0.1,
            },
            {
                "status": "FILLED",
                "flow": "entry",
                "decision_max_notional": 80.0,
                "wallet_score": 72.0,
                "topic_label": "政治",
                "topic_sample_count": 5,
                "topic_win_rate": 0.7,
                "topic_roi": 0.1,
            },
        ]
        matrix = evaluate_replay_matrix(
            samples,
            [ReplayScenario.from_settings(self.settings, name="baseline")],
            topic_filter={"加密"},
        )

        self.assertEqual(matrix["rows"][0]["sample_count"], 1)
        self.assertEqual(matrix["rows"][0]["topic_filter"], ["加密"])

    def test_samples_can_be_filtered_by_wallet_pool_version(self) -> None:
        events_path = _write_events(
            [
                {
                    "type": "order_filled",
                    "ts": 1700000000,
                    "cycle_id": "cyc-a",
                    "wallet": "0xaaa",
                    "market_slug": "btc-above-100k",
                    "token_id": "token-a",
                    "side": "BUY",
                    "decision_max_notional": 100.0,
                    "wallet_score": 72.0,
                    "topic_label": "加密",
                },
                {
                    "type": "order_filled",
                    "ts": 1700000100,
                    "cycle_id": "cyc-b",
                    "wallet": "0xbbb",
                    "market_slug": "election-market",
                    "token_id": "token-b",
                    "side": "BUY",
                    "decision_max_notional": 80.0,
                    "wallet_score": 72.0,
                    "topic_label": "政治",
                },
            ]
        )
        runtime_state_path = _write_runtime_state(
            [
                {
                    "cycle_id": "cyc-a",
                    "wallet_pool_snapshot": [
                        {"wallet": "0xaaa", "wallet_score": 81.2, "wallet_tier": "CORE"},
                    ],
                },
                {
                    "cycle_id": "cyc-b",
                    "wallet_pool_snapshot": [
                        {"wallet": "0xbbb", "wallet_score": 67.5, "wallet_tier": "TRADE"},
                    ],
                },
            ]
        )
        samples = load_replay_samples(events_path, runtime_state_path=runtime_state_path)
        pools = summarize_wallet_pools(samples)

        self.assertEqual(len(pools), 2)
        target_pool = str(pools[0]["wallet_pool_version"] or "")
        matrix = evaluate_replay_matrix(
            samples,
            [ReplayScenario.from_settings(self.settings, name="baseline")],
            wallet_pool_filter={target_pool.lower()},
        )

        self.assertEqual(matrix["rows"][0]["sample_count"], 1)
        self.assertEqual(matrix["rows"][0]["wallet_pool_filter"], [target_pool.lower()])


if __name__ == "__main__":
    unittest.main()
