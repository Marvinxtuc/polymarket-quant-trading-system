from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass

from polymarket_bot.clients.data_api import PolymarketDataClient, ClosedPosition, ResolvedMarket
from polymarket_bot.wallet_scoring import RealizedWalletMetrics, build_realized_wallet_metrics


TOPIC_RULES: tuple[tuple[str, str, tuple[str, ...]], ...] = (
    (
        "crypto",
        "加密",
        (
            "btc",
            "bitcoin",
            "eth",
            "ethereum",
            "sol",
            "doge",
            "xrp",
            "crypto",
            "token",
            "stablecoin",
            "memecoin",
            "blockchain",
        ),
    ),
    (
        "politics",
        "政治",
        (
            "election",
            "president",
            "senate",
            "house",
            "governor",
            "mayor",
            "trump",
            "biden",
            "republican",
            "democrat",
            "parliament",
            "prime-minister",
            "minister",
            "cabinet",
            "campaign",
            "vote",
        ),
    ),
    (
        "macro",
        "宏观",
        (
            "fed",
            "inflation",
            "cpi",
            "ppi",
            "gdp",
            "recession",
            "rate",
            "rates",
            "yield",
            "tariff",
            "oil",
            "gold",
            "treasury",
            "economy",
            "unemployment",
        ),
    ),
    (
        "sports",
        "体育",
        (
            "nba",
            "nfl",
            "mlb",
            "nhl",
            "fifa",
            "uefa",
            "ufc",
            "tennis",
            "golf",
            "super-bowl",
            "world-series",
            "champions-league",
            "olympics",
            "formula-1",
        ),
    ),
    (
        "tech",
        "科技",
        (
            "ai",
            "openai",
            "nvidia",
            "apple",
            "google",
            "meta",
            "microsoft",
            "tesla",
            "amazon",
            "chatgpt",
            "anthropic",
            "robot",
        ),
    ),
    (
        "world",
        "国际",
        (
            "ukraine",
            "russia",
            "china",
            "taiwan",
            "israel",
            "iran",
            "war",
            "ceasefire",
            "nato",
            "eu",
            "gaza",
        ),
    ),
    (
        "culture",
        "娱乐",
        (
            "oscar",
            "grammy",
            "movie",
            "box-office",
            "album",
            "taylor-swift",
            "celebrity",
            "tv-show",
            "emmy",
        ),
    ),
)


@dataclass(slots=True, frozen=True)
class RecentClosedMarketSample:
    market_slug: str
    outcome: str
    token_id: str
    total_bought: float
    realized_pnl: float
    roi: float
    timestamp: int
    end_date: str
    resolved: bool
    resolved_correct: bool | None
    winner_outcome: str

    def as_dict(self) -> dict[str, object]:
        return {
            "market_slug": self.market_slug,
            "outcome": self.outcome,
            "token_id": self.token_id,
            "total_bought": round(self.total_bought, 2),
            "realized_pnl": round(self.realized_pnl, 2),
            "roi": round(self.roi, 4),
            "timestamp": self.timestamp,
            "end_date": self.end_date,
            "resolved": self.resolved,
            "resolved_correct": self.resolved_correct,
            "winner_outcome": self.winner_outcome,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> RecentClosedMarketSample | None:
        try:
            market_slug = str(payload.get("market_slug") or "").strip()
            outcome = str(payload.get("outcome") or "").strip()
            token_id = str(payload.get("token_id") or "").strip()
            if not market_slug:
                return None
            resolved_correct_raw = payload.get("resolved_correct")
            resolved_correct = (
                None
                if resolved_correct_raw is None
                else bool(resolved_correct_raw)
            )
            return cls(
                market_slug=market_slug,
                outcome=outcome,
                token_id=token_id,
                total_bought=float(payload.get("total_bought") or 0.0),
                realized_pnl=float(payload.get("realized_pnl") or 0.0),
                roi=float(payload.get("roi") or 0.0),
                timestamp=int(payload.get("timestamp") or 0),
                end_date=str(payload.get("end_date") or ""),
                resolved=bool(payload.get("resolved", False)),
                resolved_correct=resolved_correct,
                winner_outcome=str(payload.get("winner_outcome") or ""),
            )
        except (TypeError, ValueError):
            return None


@dataclass(slots=True, frozen=True)
class TopicProfile:
    key: str
    label: str
    sample_count: int
    wins: int
    win_rate: float
    realized_pnl: float
    roi: float
    resolved_markets: int
    resolved_wins: int
    resolved_win_rate: float
    sample_share: float

    def as_dict(self) -> dict[str, object]:
        return {
            "key": self.key,
            "label": self.label,
            "sample_count": self.sample_count,
            "wins": self.wins,
            "win_rate": round(self.win_rate, 4),
            "realized_pnl": round(self.realized_pnl, 2),
            "roi": round(self.roi, 4),
            "resolved_markets": self.resolved_markets,
            "resolved_wins": self.resolved_wins,
            "resolved_win_rate": round(self.resolved_win_rate, 4),
            "sample_share": round(self.sample_share, 4),
        }

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> TopicProfile | None:
        try:
            key = str(payload.get("key") or "").strip()
            label = str(payload.get("label") or "").strip()
            if not key:
                return None
            return cls(
                key=key,
                label=label or key,
                sample_count=int(payload.get("sample_count") or 0),
                wins=int(payload.get("wins") or 0),
                win_rate=float(payload.get("win_rate") or 0.0),
                realized_pnl=float(payload.get("realized_pnl") or 0.0),
                roi=float(payload.get("roi") or 0.0),
                resolved_markets=int(payload.get("resolved_markets") or 0),
                resolved_wins=int(payload.get("resolved_wins") or 0),
                resolved_win_rate=float(payload.get("resolved_win_rate") or 0.0),
                sample_share=float(payload.get("sample_share") or 0.0),
            )
        except (TypeError, ValueError):
            return None


@dataclass(slots=True, frozen=True)
class WalletHistoryEntry:
    wallet: str
    refreshed_ts: int
    realized_metrics: RealizedWalletMetrics
    recent_closed_markets: tuple[RecentClosedMarketSample, ...] = ()
    topic_profiles: tuple[TopicProfile, ...] = ()

    def as_dict(self) -> dict[str, object]:
        return {
            "wallet": self.wallet,
            "refreshed_ts": self.refreshed_ts,
            "realized_metrics": self.realized_metrics.as_dict(),
            "recent_closed_markets": [row.as_dict() for row in self.recent_closed_markets],
            "topic_profiles": [row.as_dict() for row in self.topic_profiles],
        }

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> WalletHistoryEntry | None:
        wallet = str(payload.get("wallet") or "").strip().lower()
        if not wallet:
            return None
        refreshed_ts = int(payload.get("refreshed_ts") or 0)
        raw_metrics = payload.get("realized_metrics")
        if not isinstance(raw_metrics, dict):
            return None
        try:
            metrics = RealizedWalletMetrics(
                closed_positions=int(raw_metrics.get("closed_positions") or 0),
                wins=int(raw_metrics.get("wins") or 0),
                resolved_markets=int(raw_metrics.get("resolved_markets") or 0),
                resolved_wins=int(raw_metrics.get("resolved_wins") or 0),
                total_bought=float(raw_metrics.get("total_bought") or 0.0),
                realized_pnl=float(raw_metrics.get("realized_pnl") or 0.0),
                gross_profit=float(raw_metrics.get("gross_profit") or 0.0),
                gross_loss=float(raw_metrics.get("gross_loss") or 0.0),
                win_rate=float(raw_metrics.get("win_rate") or 0.0),
                resolved_win_rate=float(raw_metrics.get("resolved_win_rate") or 0.0),
                roi=float(raw_metrics.get("roi") or 0.0),
                profit_factor=float(raw_metrics.get("profit_factor") or 0.0),
            )
        except (TypeError, ValueError):
            return None
        recent_rows: list[RecentClosedMarketSample] = []
        raw_recent = payload.get("recent_closed_markets")
        if isinstance(raw_recent, list):
            for row in raw_recent:
                if not isinstance(row, dict):
                    continue
                recent_row = RecentClosedMarketSample.from_dict(row)
                if recent_row is None:
                    continue
                recent_rows.append(recent_row)
        topic_rows: list[TopicProfile] = []
        raw_topics = payload.get("topic_profiles")
        if isinstance(raw_topics, list):
            for row in raw_topics:
                if not isinstance(row, dict):
                    continue
                topic_row = TopicProfile.from_dict(row)
                if topic_row is None:
                    continue
                topic_rows.append(topic_row)
        return cls(
            wallet=wallet,
            refreshed_ts=refreshed_ts,
            realized_metrics=metrics,
            recent_closed_markets=tuple(recent_rows),
            topic_profiles=tuple(topic_rows),
        )


def infer_market_topic(market_slug: str) -> tuple[str, str]:
    slug = str(market_slug or "").strip().lower()
    if not slug:
        return ("other", "其他")
    for key, label, keywords in TOPIC_RULES:
        if any(keyword in slug for keyword in keywords):
            return (key, label)
    return ("other", "其他")


def build_recent_closed_market_samples(
    closed_positions: list[ClosedPosition],
    resolution_map: dict[str, ResolvedMarket] | None = None,
    *,
    limit: int = 5,
) -> tuple[RecentClosedMarketSample, ...]:
    rows: list[RecentClosedMarketSample] = []
    for position in sorted(
        closed_positions,
        key=lambda row: (int(getattr(row, "timestamp", 0) or 0), str(getattr(row, "end_date", "") or "")),
        reverse=True,
    )[: max(1, int(limit))]:
        total_bought = max(0.0, float(position.total_bought))
        realized_pnl = float(position.realized_pnl)
        roi = realized_pnl / total_bought if total_bought > 0 else 0.0
        resolved = None if resolution_map is None else resolution_map.get(position.condition_id)
        is_resolved = bool(resolved and resolved.closed and resolved.winner_token_id)
        resolved_correct = None
        winner_outcome = ""
        if is_resolved and resolved is not None:
            resolved_correct = position.token_id == resolved.winner_token_id
            winner_outcome = str(resolved.winner_outcome or "")
        rows.append(
            RecentClosedMarketSample(
                market_slug=str(position.market_slug or position.condition_id or position.token_id),
                outcome=str(position.outcome or ""),
                token_id=str(position.token_id or ""),
                total_bought=round(total_bought, 2),
                realized_pnl=round(realized_pnl, 2),
                roi=round(roi, 4),
                timestamp=int(position.timestamp or 0),
                end_date=str(position.end_date or ""),
                resolved=is_resolved,
                resolved_correct=resolved_correct,
                winner_outcome=winner_outcome,
            )
        )
    return tuple(rows)


def build_topic_profiles(
    closed_positions: list[ClosedPosition],
    resolution_map: dict[str, ResolvedMarket] | None = None,
    *,
    limit: int = 4,
) -> tuple[TopicProfile, ...]:
    if not closed_positions:
        return ()

    grouped: dict[str, dict[str, object]] = {}
    total_samples = 0
    for position in closed_positions:
        topic_key, topic_label = infer_market_topic(position.market_slug)
        bucket = grouped.setdefault(
            topic_key,
            {
                "key": topic_key,
                "label": topic_label,
                "sample_count": 0,
                "wins": 0,
                "realized_pnl": 0.0,
                "total_bought": 0.0,
                "resolved_markets": 0,
                "resolved_wins": 0,
            },
        )
        total_samples += 1
        bucket["sample_count"] = int(bucket["sample_count"]) + 1
        pnl = float(position.realized_pnl)
        bucket["realized_pnl"] = float(bucket["realized_pnl"]) + pnl
        bucket["total_bought"] = float(bucket["total_bought"]) + max(0.0, float(position.total_bought))
        if pnl > 0:
            bucket["wins"] = int(bucket["wins"]) + 1

        resolved = None if resolution_map is None else resolution_map.get(position.condition_id)
        if resolved is not None and resolved.closed and resolved.winner_token_id:
            bucket["resolved_markets"] = int(bucket["resolved_markets"]) + 1
            if position.token_id == resolved.winner_token_id:
                bucket["resolved_wins"] = int(bucket["resolved_wins"]) + 1

    rows: list[TopicProfile] = []
    for bucket in grouped.values():
        sample_count = int(bucket["sample_count"])
        wins = int(bucket["wins"])
        total_bought = float(bucket["total_bought"])
        realized_pnl = float(bucket["realized_pnl"])
        resolved_markets = int(bucket["resolved_markets"])
        resolved_wins = int(bucket["resolved_wins"])
        rows.append(
            TopicProfile(
                key=str(bucket["key"]),
                label=str(bucket["label"]),
                sample_count=sample_count,
                wins=wins,
                win_rate=(wins / sample_count) if sample_count else 0.0,
                realized_pnl=realized_pnl,
                roi=(realized_pnl / total_bought) if total_bought > 0 else 0.0,
                resolved_markets=resolved_markets,
                resolved_wins=resolved_wins,
                resolved_win_rate=(resolved_wins / resolved_markets) if resolved_markets else 0.0,
                sample_share=(sample_count / total_samples) if total_samples else 0.0,
            )
        )
    rows.sort(
        key=lambda row: (
            int(row.sample_count),
            float(row.realized_pnl),
            float(row.win_rate),
            row.label,
        ),
        reverse=True,
    )
    return tuple(rows[: max(1, int(limit))])


class WalletHistoryStore:
    def __init__(
        self,
        *,
        client: PolymarketDataClient,
        cache_path: str,
        refresh_seconds: int = 1800,
        max_wallets: int = 12,
        closed_limit: int = 20,
        resolution_limit: int = 8,
    ) -> None:
        self.client = client
        self.cache_path = cache_path
        self.refresh_seconds = max(60, int(refresh_seconds))
        self.max_wallets = max(1, int(max_wallets))
        self.closed_limit = min(50, max(1, int(closed_limit)))
        self.resolution_limit = max(0, int(resolution_limit))
        self.log = logging.getLogger("polybot.wallet_history")
        self._entries: dict[str, WalletHistoryEntry] = {}
        self._load_cache()

    def sync_wallets(
        self,
        wallets: list[str],
        *,
        max_wallets: int | None = None,
    ) -> tuple[
        dict[str, RealizedWalletMetrics],
        dict[str, int],
        dict[str, list[dict[str, object]]],
        dict[str, list[dict[str, object]]],
    ]:
        targets: list[str] = []
        seen: set[str] = set()
        target_limit = self.max_wallets if max_wallets is None else max(1, int(max_wallets))
        for raw_wallet in wallets:
            wallet = str(raw_wallet).strip().lower()
            if not wallet or wallet in seen:
                continue
            targets.append(wallet)
            seen.add(wallet)
            if len(targets) >= target_limit:
                break

        now = int(time.time())
        changed = False
        for wallet in targets:
            if not self._needs_refresh(wallet, now):
                continue
            try:
                self._entries[wallet] = self._fetch_wallet_history(wallet, now)
                changed = True
            except Exception as exc:
                self.log.warning("wallet_history_refresh failed wallet=%s err=%s", wallet, exc)

        if changed:
            self._persist_cache()

        metrics: dict[str, RealizedWalletMetrics] = {}
        refreshed_ts: dict[str, int] = {}
        recent_closed_markets: dict[str, list[dict[str, object]]] = {}
        topic_profiles: dict[str, list[dict[str, object]]] = {}
        for wallet in targets:
            entry = self._entries.get(wallet)
            if entry is None:
                continue
            metrics[wallet] = entry.realized_metrics
            refreshed_ts[wallet] = entry.refreshed_ts
            recent_closed_markets[wallet] = [row.as_dict() for row in entry.recent_closed_markets]
            topic_profiles[wallet] = [row.as_dict() for row in entry.topic_profiles]
        return metrics, refreshed_ts, recent_closed_markets, topic_profiles

    def peek_wallets(
        self,
        wallets: list[str],
    ) -> tuple[
        dict[str, RealizedWalletMetrics],
        dict[str, int],
        dict[str, list[dict[str, object]]],
        dict[str, list[dict[str, object]]],
    ]:
        metrics: dict[str, RealizedWalletMetrics] = {}
        refreshed_ts: dict[str, int] = {}
        recent_closed_markets: dict[str, list[dict[str, object]]] = {}
        topic_profiles: dict[str, list[dict[str, object]]] = {}
        seen: set[str] = set()
        for raw_wallet in wallets:
            wallet = str(raw_wallet).strip().lower()
            if not wallet or wallet in seen:
                continue
            seen.add(wallet)
            entry = self._entries.get(wallet)
            if entry is None:
                continue
            metrics[wallet] = entry.realized_metrics
            refreshed_ts[wallet] = entry.refreshed_ts
            recent_closed_markets[wallet] = [row.as_dict() for row in entry.recent_closed_markets]
            topic_profiles[wallet] = [row.as_dict() for row in entry.topic_profiles]
        return metrics, refreshed_ts, recent_closed_markets, topic_profiles

    def _needs_refresh(self, wallet: str, now: int) -> bool:
        entry = self._entries.get(wallet)
        if entry is None:
            return True
        return (now - entry.refreshed_ts) >= self.refresh_seconds

    def _fetch_wallet_history(self, wallet: str, now: int) -> WalletHistoryEntry:
        closed_positions = self.client.get_closed_positions(wallet, limit=self.closed_limit)
        condition_slug_map = {
            position.condition_id: position.market_slug
            for position in closed_positions
            if position.condition_id and position.market_slug
        }
        condition_ids = [
            position.condition_id
            for position in closed_positions
            if position.condition_id
        ][: self.resolution_limit]
        resolution_map = (
            self.client.build_resolution_map(
                set(condition_ids),
                market_slugs=condition_slug_map,
                max_pages=40,
            )
            if condition_ids
            else {}
        )
        metrics = build_realized_wallet_metrics(closed_positions, resolution_map if resolution_map else None)
        recent_closed_markets = build_recent_closed_market_samples(
            closed_positions,
            resolution_map if resolution_map else None,
            limit=min(5, self.closed_limit),
        )
        topic_profiles = build_topic_profiles(
            closed_positions,
            resolution_map if resolution_map else None,
            limit=4,
        )
        return WalletHistoryEntry(
            wallet=wallet,
            refreshed_ts=now,
            realized_metrics=metrics,
            recent_closed_markets=recent_closed_markets,
            topic_profiles=topic_profiles,
        )

    def _load_cache(self) -> None:
        if not self.cache_path:
            return
        try:
            with open(self.cache_path, "r", encoding="utf-8") as f:
                payload = json.load(f)
        except FileNotFoundError:
            return
        except Exception as exc:
            self.log.warning("wallet_history_load failed path=%s err=%s", self.cache_path, exc)
            return

        entries = payload.get("wallets") if isinstance(payload, dict) else None
        if not isinstance(entries, list):
            return

        for row in entries:
            if not isinstance(row, dict):
                continue
            entry = WalletHistoryEntry.from_dict(row)
            if entry is None:
                continue
            self._entries[entry.wallet] = entry

    def _persist_cache(self) -> None:
        if not self.cache_path:
            return
        payload = {
            "ts": int(time.time()),
            "version": 1,
            "wallets": [entry.as_dict() for entry in self._entries.values()],
        }
        try:
            os.makedirs(os.path.dirname(self.cache_path), exist_ok=True)
            tmp_path = f"{self.cache_path}.tmp"
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False)
            os.replace(tmp_path, self.cache_path)
        except Exception as exc:
            self.log.warning("wallet_history_persist failed path=%s err=%s", self.cache_path, exc)
