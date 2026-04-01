from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from polymarket_bot.clients.data_api import ClosedPosition, ResolvedMarket


def _clamp(value: float, lower: float = 0.0, upper: float = 1.0) -> float:
    return max(lower, min(upper, value))


@dataclass(slots=True, frozen=True)
class RealizedWalletMetrics:
    closed_positions: int
    wins: int
    resolved_markets: int
    resolved_wins: int
    total_bought: float
    realized_pnl: float
    gross_profit: float
    gross_loss: float
    win_rate: float
    resolved_win_rate: float
    roi: float
    profit_factor: float

    def as_dict(self) -> dict[str, float | int]:
        return {
            "closed_positions": self.closed_positions,
            "wins": self.wins,
            "resolved_markets": self.resolved_markets,
            "resolved_wins": self.resolved_wins,
            "total_bought": round(self.total_bought, 2),
            "realized_pnl": round(self.realized_pnl, 2),
            "gross_profit": round(self.gross_profit, 2),
            "gross_loss": round(self.gross_loss, 2),
            "win_rate": round(self.win_rate, 4),
            "resolved_win_rate": round(self.resolved_win_rate, 4),
            "roi": round(self.roi, 4),
            "profit_factor": round(self.profit_factor, 4),
        }


@dataclass(slots=True, frozen=True)
class WalletScore:
    score: float
    tier: str
    components: dict[str, float]
    summary: str
    activity_known: bool

    def as_dict(self) -> dict[str, object]:
        return {
            "wallet_score": self.score,
            "wallet_tier": self.tier,
            "score_components": dict(self.components),
            "score_summary": self.summary,
            "activity_known": self.activity_known,
        }


def build_realized_wallet_metrics(
    closed_positions: Sequence[ClosedPosition],
    resolution_map: Mapping[str, ResolvedMarket] | None = None,
) -> RealizedWalletMetrics:
    closed_count = 0
    wins = 0
    total_bought = 0.0
    realized_pnl = 0.0
    gross_profit = 0.0
    gross_loss = 0.0
    resolved_markets = 0
    resolved_wins = 0

    for position in closed_positions:
        closed_count += 1
        total_bought += max(0.0, float(position.total_bought))
        pnl = float(position.realized_pnl)
        realized_pnl += pnl
        if pnl > 0:
            wins += 1
            gross_profit += pnl
        elif pnl < 0:
            gross_loss += abs(pnl)

        if resolution_map is None:
            continue
        resolved = resolution_map.get(position.condition_id)
        if resolved is None or not resolved.closed or not resolved.winner_token_id:
            continue
        resolved_markets += 1
        if position.token_id == resolved.winner_token_id:
            resolved_wins += 1

    win_rate = wins / closed_count if closed_count else 0.0
    resolved_win_rate = resolved_wins / resolved_markets if resolved_markets else 0.0
    roi = realized_pnl / total_bought if total_bought > 0 else 0.0
    if gross_loss > 0:
        profit_factor = gross_profit / gross_loss
    elif gross_profit > 0:
        profit_factor = 5.0
    else:
        profit_factor = 0.0

    return RealizedWalletMetrics(
        closed_positions=closed_count,
        wins=wins,
        resolved_markets=resolved_markets,
        resolved_wins=resolved_wins,
        total_bought=round(total_bought, 2),
        realized_pnl=round(realized_pnl, 2),
        gross_profit=round(gross_profit, 2),
        gross_loss=round(gross_loss, 2),
        win_rate=round(win_rate, 4),
        resolved_win_rate=round(resolved_win_rate, 4),
        roi=round(roi, 4),
        profit_factor=round(profit_factor, 4),
    )


class SmartWalletScorer:
    """Heuristic scorer for the current wallet-follower stack.

    This is intentionally a proxy score: it relies on observable position shape
    and recent activity counts until we ingest full historical resolved PnL.
    When realized history is available, it is blended in with a sample-size-aware
    weight so thin history does not dominate the live score.
    """

    def __init__(
        self,
        *,
        target_notional_usd: float = 10_000.0,
        target_recent_events: int = 12,
        target_active_positions: int = 8,
        target_unique_markets: int = 10,
        min_realized_sample: int = 5,
        strong_realized_sample: int = 15,
        strong_resolved_sample: int = 10,
    ) -> None:
        self.target_notional_usd = max(1.0, float(target_notional_usd))
        self.target_recent_events = max(1, int(target_recent_events))
        self.target_active_positions = max(1, int(target_active_positions))
        self.target_unique_markets = max(1, int(target_unique_markets))
        self.min_realized_sample = max(1, int(min_realized_sample))
        self.strong_realized_sample = max(self.min_realized_sample, int(strong_realized_sample))
        self.strong_resolved_sample = max(1, int(strong_resolved_sample))

    def score_wallet(
        self,
        *,
        total_notional_usd: float,
        active_positions: int,
        unique_markets: int,
        top_market_share: float,
        recent_activity_events: int | None = None,
        realized_metrics: RealizedWalletMetrics | None = None,
    ) -> WalletScore:
        safe_notional = max(0.0, float(total_notional_usd))
        safe_positions = max(0, int(active_positions))
        safe_unique_markets = max(0, int(unique_markets))
        safe_top_share = _clamp(float(top_market_share), 0.0, 1.0)

        notional_score = (
            _clamp(math.log1p(safe_notional) / math.log1p(self.target_notional_usd)) * 30.0
        )
        positions_score = _clamp(safe_positions / self.target_active_positions) * 15.0
        unique_markets_score = _clamp(safe_unique_markets / self.target_unique_markets) * 15.0
        concentration_score = _clamp((0.95 - safe_top_share) / 0.55) * 20.0

        activity_known = recent_activity_events is not None
        if activity_known:
            safe_recent_events = max(0, int(recent_activity_events or 0))
            activity_score = _clamp(safe_recent_events / self.target_recent_events) * 20.0
            activity_label = f"{safe_recent_events} recent events"
        else:
            activity_score = 10.0
            activity_label = "recent activity unknown"

        proxy_score = round(
            notional_score
            + positions_score
            + unique_markets_score
            + concentration_score
            + activity_score,
            2,
        )
        score = proxy_score
        components = {
            "notional": round(notional_score, 2),
            "positions": round(positions_score, 2),
            "unique_markets": round(unique_markets_score, 2),
            "concentration": round(concentration_score, 2),
            "activity": round(activity_score, 2),
        }
        history_label = ""
        if realized_metrics is not None and realized_metrics.closed_positions > 0:
            realized_score, realized_components = self._score_realized_history(realized_metrics)
            history_weight = self._history_weight(realized_metrics)
            score = round((proxy_score * (1.0 - history_weight)) + (realized_score * history_weight), 2)
            components.update(realized_components)
            history_label = (
                f" | win {realized_metrics.win_rate:.0%}"
                f" | roi {realized_metrics.roi:+.0%}"
                f" | closed {realized_metrics.closed_positions}"
            )
            if realized_metrics.resolved_markets > 0:
                history_label += f" | resolved {realized_metrics.resolved_win_rate:.0%}"
            elif realized_metrics.closed_positions < self.min_realized_sample:
                history_label += " | history thin"

        tier = self._tier_for_score(score)
        summary = (
            f"{tier} {score:.1f} | {safe_positions} pos | {safe_unique_markets} mkts | "
            f"top {safe_top_share:.0%} | {activity_label}{history_label}"
        )

        return WalletScore(
            score=score,
            tier=tier,
            components=components,
            summary=summary,
            activity_known=activity_known,
        )

    @staticmethod
    def _tier_for_score(score: float) -> str:
        if score >= 80.0:
            return "CORE"
        if score >= 65.0:
            return "TRADE"
        if score >= 50.0:
            return "WATCH"
        return "LOW"

    def _history_weight(self, realized_metrics: RealizedWalletMetrics) -> float:
        if realized_metrics.closed_positions < self.min_realized_sample:
            return 0.0
        if (
            realized_metrics.closed_positions >= self.strong_realized_sample
            or realized_metrics.resolved_markets >= self.strong_resolved_sample
        ):
            return 0.65
        return 0.35

    def _score_realized_history(self, realized_metrics: RealizedWalletMetrics) -> tuple[float, dict[str, float]]:
        win_rate_score = _clamp(realized_metrics.win_rate / 0.65) * 35.0
        roi_score = _clamp((realized_metrics.roi + 0.05) / 0.30) * 30.0
        profit_factor_score = _clamp(realized_metrics.profit_factor / 2.0) * 20.0
        if realized_metrics.resolved_markets > 0:
            resolved_score = _clamp(realized_metrics.resolved_win_rate / 0.70) * 15.0
        else:
            resolved_score = 7.5
        realized_score = round(win_rate_score + roi_score + profit_factor_score + resolved_score, 2)
        return realized_score, {
            "history_win_rate": round(win_rate_score, 2),
            "history_roi": round(roi_score, 2),
            "history_profit_factor": round(profit_factor_score, 2),
            "history_resolution": round(resolved_score, 2),
        }
