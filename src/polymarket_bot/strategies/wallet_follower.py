from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
import re
import time

from polymarket_bot.clients.data_api import ActivityEvent, MarketMetadata, PolymarketDataClient, Position, TradeFill
from polymarket_bot.types import Signal
from polymarket_bot.wallet_history import infer_market_topic
from polymarket_bot.wallet_scoring import RealizedWalletMetrics, SmartWalletScorer

_MARKET_WINDOW_PATTERN = re.compile(r"-(5m|15m|30m|1h)-(\d{10})$")
_MARKET_WINDOW_SECONDS = {
    "5m": 300,
    "15m": 900,
    "30m": 1800,
    "1h": 3600,
}
_MARKET_METADATA_CACHE_TTL_SECONDS = 300
_SHORT_WINDOW_EDGE_ASK = 0.97


@dataclass(slots=True)
class PositionState:
    size: float
    notional: float
    price: float
    updated_ts: int
    condition_id: str
    market_slug: str
    outcome: str


@dataclass(slots=True)
class WalletUniverseRule:
    min_active_positions: int
    min_unique_markets: int
    min_total_notional_usd: float
    max_top_market_share: float


@dataclass(slots=True)
class WalletTradeEvent:
    source: str
    wallet: str
    token_id: str
    condition_id: str
    market_slug: str
    outcome: str
    side: str
    price: float
    size: float
    notional: float
    timestamp: int
    event_type: str = ""
    tx_hash: str = ""

    @property
    def dedupe_key(self) -> str:
        tx_hash = str(self.tx_hash or "").strip().lower()
        if tx_hash:
            return f"{tx_hash}:{self.token_id}:{self.side}"
        return (
            f"{self.wallet}:{self.token_id}:{self.side}:{self.timestamp}:"
            f"{self.price:.6f}:{self.size:.6f}:{self.notional:.6f}"
        )


@dataclass(slots=True)
class RawWalletEvent:
    source: str
    wallet: str
    token_id: str
    condition_id: str
    market_slug: str
    outcome: str
    side: str
    price: float
    size: float
    notional: float
    timestamp: int
    event_type: str = ""
    tx_hash: str = ""
    trigger_type: str = ""

    @property
    def dedupe_key(self) -> str:
        tx_hash = str(self.tx_hash or "").strip().lower()
        if tx_hash:
            return f"{tx_hash}:{self.token_id}:{self.side}"
        return (
            f"{self.wallet}:{self.token_id}:{self.side}:{self.timestamp}:"
            f"{self.price:.6f}:{self.size:.6f}:{self.notional:.6f}"
        )


WalletTradeEvent = RawWalletEvent


@dataclass(slots=True)
class WalletCandidateContext:
    wallet: str
    token_id: str
    condition_id: str
    market_slug: str
    outcome: str
    side: str
    trigger_type: str
    confidence: float
    price_hint: float
    observed_size: float
    observed_notional: float
    timestamp: datetime
    wallet_score: float
    wallet_tier: str
    wallet_score_summary: str
    topic_key: str
    topic_label: str
    topic_sample_count: int
    topic_win_rate: float
    topic_roi: float
    topic_resolved_win_rate: float
    topic_score_summary: str
    position_action: str
    position_action_label: str
    exit_fraction: float = 0.0
    exit_reason: str = ""
    cross_wallet_exit: bool = False
    source_event: RawWalletEvent | None = None
    previous_position: PositionState | None = None
    current_position: Position | None = None
    source_wallet_count: int = 1
    resonance_wallets: tuple[str, ...] = ()
    resonance_boost: float = 0.0


@dataclass(slots=True)
class WalletFollowerStrategy:
    client: PolymarketDataClient
    min_increase_usd: float
    max_signals_per_cycle: int
    min_active_positions: int
    min_unique_markets: int
    min_total_notional_usd: float
    max_top_market_share: float
    min_wallet_score: float = 50.0
    min_decrease_usd: float = 200.0
    follow_wallet_exits: bool = True
    resonance_exit_enabled: bool = True
    resonance_min_wallets: int = 2
    resonance_min_wallet_score: float = 65.0
    resonance_trim_fraction: float = 0.35
    resonance_core_exit_fraction: float = 0.6
    signal_source: str = "hybrid"
    signal_lookback_seconds: int = 900
    signal_page_size: int = 100
    signal_max_pages: int = 2
    live_buy_max_chase_pct: float = 12.0
    scorer: SmartWalletScorer = field(default_factory=SmartWalletScorer)
    _state: dict[str, dict[str, PositionState]] = field(default_factory=dict)
    _latest_wallet_positions: dict[str, list[Position]] = field(default_factory=dict)
    _wallet_activity_counts: dict[str, int] = field(default_factory=dict)
    _wallet_activity_available: bool = False
    _wallet_realized_metrics: dict[str, RealizedWalletMetrics] = field(default_factory=dict)
    _wallet_history_refreshed_ts: dict[str, int] = field(default_factory=dict)
    _wallet_recent_closed_markets: dict[str, list[dict[str, object]]] = field(default_factory=dict)
    _wallet_topic_profiles: dict[str, list[dict[str, object]]] = field(default_factory=dict)
    _wallet_selection_context: dict[str, dict[str, object]] = field(default_factory=dict)
    _last_wallet_metrics: dict[str, dict[str, object]] = field(default_factory=dict)
    _wallet_event_watermarks: dict[str, int] = field(default_factory=dict)
    _wallet_event_watermark_keys: dict[str, set[str]] = field(default_factory=dict)
    _wallet_seen_tokens: dict[str, set[str]] = field(default_factory=dict)
    _live_market_cache: dict[str, tuple[int, float, float]] = field(default_factory=dict)
    _market_metadata_cache: dict[str, tuple[int, MarketMetadata | None]] = field(default_factory=dict)

    def update_wallet_activity_counts(self, counts: Mapping[str, int], *, available: bool = True) -> None:
        normalized: dict[str, int] = {}
        for wallet, count in counts.items():
            key = str(wallet).strip().lower()
            if not key:
                continue
            normalized[key] = max(0, int(count))
        self._wallet_activity_counts = normalized
        self._wallet_activity_available = bool(available)

    def update_wallet_realized_metrics(
        self,
        metrics: Mapping[str, RealizedWalletMetrics],
        *,
        refreshed_ts: Mapping[str, int] | None = None,
        recent_closed_markets: Mapping[str, list[dict[str, object]]] | None = None,
        topic_profiles: Mapping[str, list[dict[str, object]]] | None = None,
    ) -> None:
        normalized: dict[str, RealizedWalletMetrics] = {}
        for wallet, entry in metrics.items():
            key = str(wallet).strip().lower()
            if not key:
                continue
            normalized[key] = entry
        self._wallet_realized_metrics = normalized

        timestamps: dict[str, int] = {}
        for wallet, value in (refreshed_ts or {}).items():
            key = str(wallet).strip().lower()
            if not key:
                continue
            timestamps[key] = max(0, int(value))
        self._wallet_history_refreshed_ts = timestamps

        recent_rows: dict[str, list[dict[str, object]]] = {}
        for wallet, rows in (recent_closed_markets or {}).items():
            key = str(wallet).strip().lower()
            if not key:
                continue
            normalized_rows: list[dict[str, object]] = []
            for row in rows:
                if not isinstance(row, dict):
                    continue
                normalized_rows.append(dict(row))
            recent_rows[key] = normalized_rows
        self._wallet_recent_closed_markets = recent_rows

        topic_rows: dict[str, list[dict[str, object]]] = {}
        for wallet, rows in (topic_profiles or {}).items():
            key = str(wallet).strip().lower()
            if not key:
                continue
            normalized_topics: list[dict[str, object]] = []
            for row in rows:
                if not isinstance(row, dict):
                    continue
                normalized_topics.append(dict(row))
            topic_rows[key] = normalized_topics
        self._wallet_topic_profiles = topic_rows

    def update_wallet_selection_context(
        self,
        context: Mapping[str, Mapping[str, object]],
    ) -> None:
        normalized: dict[str, dict[str, object]] = {}
        for wallet, row in context.items():
            key = str(wallet).strip().lower()
            if not key:
                continue
            normalized[key] = dict(row)
        self._wallet_selection_context = normalized

    def _wallet_rule(self) -> WalletUniverseRule:
        return WalletUniverseRule(
            min_active_positions=self.min_active_positions,
            min_unique_markets=self.min_unique_markets,
            min_total_notional_usd=self.min_total_notional_usd,
            max_top_market_share=self.max_top_market_share,
        )

    def _screen_wallets(self, wallets: list[str]) -> dict[str, list[Position]]:
        rule = self._wallet_rule()
        selected: dict[str, list[Position]] = {}
        self._last_wallet_metrics = {}
        self._latest_wallet_positions = {}

        for wallet in wallets:
            positions = self.client.get_active_positions(wallet)
            self._latest_wallet_positions[wallet] = list(positions)
            if not positions:
                continue

            active_positions = len(positions)
            unique_markets = len({p.market_slug or p.token_id for p in positions})
            total_notional = sum(max(0.0, p.notional) for p in positions)
            if total_notional <= 0:
                continue

            market_notional: dict[str, float] = {}
            for p in positions:
                key = p.market_slug or p.token_id
                market_notional[key] = market_notional.get(key, 0.0) + max(0.0, p.notional)
            top_market_share = max(market_notional.values()) / total_notional

            if active_positions < rule.min_active_positions:
                continue
            if unique_markets < rule.min_unique_markets:
                continue
            if total_notional < rule.min_total_notional_usd:
                continue
            if top_market_share > rule.max_top_market_share:
                continue

            recent_activity_events: int | None = None
            if self._wallet_activity_available:
                recent_activity_events = self._wallet_activity_counts.get(wallet, 0)
            realized_metrics = self._wallet_realized_metrics.get(wallet)
            selection_context = dict(self._wallet_selection_context.get(wallet, {}))
            wallet_score = self.scorer.score_wallet(
                total_notional_usd=total_notional,
                active_positions=active_positions,
                unique_markets=unique_markets,
                top_market_share=top_market_share,
                recent_activity_events=recent_activity_events,
                realized_metrics=realized_metrics,
            )
            trading_enabled = wallet_score.score >= self.min_wallet_score
            history_metrics = realized_metrics.as_dict() if realized_metrics is not None else {}
            self._last_wallet_metrics[wallet] = {
                "positions": active_positions,
                "unique_markets": unique_markets,
                "total_notional": total_notional,
                "top_market_share": top_market_share,
                "recent_activity_events": recent_activity_events,
                "history_available": bool(realized_metrics and realized_metrics.closed_positions > 0),
                "history_refresh_ts": int(self._wallet_history_refreshed_ts.get(wallet, 0)),
                "realized_metrics": history_metrics,
                "recent_closed_markets": list(self._wallet_recent_closed_markets.get(wallet, [])),
                "topic_profiles": list(self._wallet_topic_profiles.get(wallet, [])),
                "trading_enabled": trading_enabled,
                "discovery_activity_events": int(selection_context.get("discovery_activity_events") or 0),
                "discovery_priority_score": float(selection_context.get("discovery_priority_score") or 0.0),
                "discovery_history_bonus": float(selection_context.get("discovery_history_bonus") or 0.0),
                "discovery_topic_bonus": float(selection_context.get("discovery_topic_bonus") or 0.0),
                "discovery_priority_rank": int(selection_context.get("discovery_priority_rank") or 0),
                "discovery_priority_reason": str(selection_context.get("discovery_priority_reason") or ""),
                "discovery_best_topic": str(selection_context.get("discovery_best_topic") or ""),
                **history_metrics,
                **wallet_score.as_dict(),
            }
            if not trading_enabled:
                continue
            selected[wallet] = positions

        return selected

    def latest_wallet_metrics(self) -> dict[str, dict[str, object]]:
        return self._last_wallet_metrics

    @staticmethod
    def _topic_profile_for_market(
        market_slug: str,
        topic_profiles: list[dict[str, object]],
    ) -> tuple[str, str, dict[str, object] | None]:
        topic_key, topic_label = infer_market_topic(market_slug)
        for row in topic_profiles:
            if str(row.get("key") or "").strip().lower() == topic_key:
                return topic_key, topic_label, row
        return topic_key, topic_label, None

    @staticmethod
    def _topic_summary(topic_label: str, profile: dict[str, object] | None) -> str:
        if profile is None:
            return f"{topic_label} | no topic history"
        return (
            f"{topic_label} | {int(profile.get('sample_count') or 0)} samples"
            f" | roi {float(profile.get('roi') or 0.0):+.0%}"
            f" | win {float(profile.get('win_rate') or 0.0):.0%}"
        )

    @staticmethod
    def _event_timestamp(value: int) -> datetime:
        if value > 0:
            return datetime.fromtimestamp(value, tz=timezone.utc)
        return datetime.now(tz=timezone.utc)

    def _normalized_signal_source(self) -> str:
        mode = str(self.signal_source or "").strip().lower()
        if mode in {"positions", "trades", "activity", "hybrid"}:
            return mode
        return "positions"

    def _event_signal_enabled(self) -> bool:
        return self._normalized_signal_source() != "positions"

    def _event_cutoff_ts(self, wallet: str) -> int:
        last_seen = int(self._wallet_event_watermarks.get(wallet, 0))
        if last_seen > 0:
            return last_seen
        now_ts = int(datetime.now(tz=timezone.utc).timestamp())
        return max(0, now_ts - max(60, int(self.signal_lookback_seconds)))

    def _prime_wallet_event_cursor(self, wallet: str) -> None:
        self._wallet_event_watermarks[wallet] = int(datetime.now(tz=timezone.utc).timestamp())
        self._wallet_event_watermark_keys[wallet] = set()

    def _update_wallet_event_cursor(self, wallet: str, events: list[WalletTradeEvent]) -> list[WalletTradeEvent]:
        if not events:
            return []

        last_seen = int(self._wallet_event_watermarks.get(wallet, 0))
        seen_keys = set(self._wallet_event_watermark_keys.get(wallet, set()))
        new_events: list[WalletTradeEvent] = []
        max_ts = last_seen
        max_ts_keys = set(seen_keys)

        for event in events:
            if event.timestamp < last_seen:
                continue
            key = event.dedupe_key
            if event.timestamp == last_seen and key in seen_keys:
                continue
            new_events.append(event)
            if event.timestamp > max_ts:
                max_ts = event.timestamp
                max_ts_keys = {key}
            elif event.timestamp == max_ts:
                max_ts_keys.add(key)

        if max_ts > 0:
            self._wallet_event_watermarks[wallet] = max_ts
            self._wallet_event_watermark_keys[wallet] = max_ts_keys
        return new_events

    @staticmethod
    def _trade_to_event(trade: TradeFill) -> WalletTradeEvent | None:
        side = str(trade.side or "").strip().upper()
        if side not in {"BUY", "SELL"}:
            return None
        size = max(0.0, float(trade.size or 0.0))
        price = max(0.0, float(trade.price or 0.0))
        notional = max(price * size, 0.0)
        token = str(trade.token_id or "").strip()
        if not token or size <= 0.0 or price <= 0.0 or notional <= 0.0:
            return None
        return WalletTradeEvent(
            source="trades",
            wallet=str(trade.wallet or "").strip().lower(),
            token_id=token,
            condition_id=str(trade.condition_id or "").strip(),
            market_slug=str(trade.market_slug or ""),
            outcome=str(trade.outcome or ""),
            side=side,
            price=price,
            size=size,
            notional=notional,
            timestamp=max(0, int(trade.timestamp or 0)),
            event_type="TRADE",
            tx_hash=str(trade.tx_hash or ""),
        )

    @staticmethod
    def _activity_to_event(event: ActivityEvent) -> WalletTradeEvent | None:
        side = str(event.side or "").strip().upper()
        if side not in {"BUY", "SELL"}:
            return None
        size = max(0.0, float(event.size or 0.0))
        price = max(0.0, float(event.price or 0.0))
        notional = max(float(event.usdc_size or 0.0), price * size)
        token = str(event.token_id or "").strip()
        if not token or size <= 0.0 or price <= 0.0 or notional <= 0.0:
            return None
        return WalletTradeEvent(
            source="activity",
            wallet=str(event.wallet or "").strip().lower(),
            token_id=token,
            condition_id=str(event.condition_id or "").strip(),
            market_slug=str(event.market_slug or ""),
            outcome=str(event.outcome or ""),
            side=side,
            price=price,
            size=size,
            notional=notional,
            timestamp=max(0, int(event.timestamp or 0)),
            event_type=str(event.activity_type or "").strip().upper(),
            tx_hash=str(event.tx_hash or ""),
        )

    def _load_wallet_trade_events(self, wallet: str, cutoff_ts: int) -> list[WalletTradeEvent]:
        events: list[WalletTradeEvent] = []
        page_size = max(1, int(self.signal_page_size))
        max_pages = max(1, int(self.signal_max_pages))
        for page_index in range(max_pages):
            page = self.client.get_user_trades(wallet, limit=page_size, offset=page_index * page_size)
            if not page:
                break
            hit_older_row = False
            for trade in page:
                parsed = self._trade_to_event(trade)
                if parsed is None:
                    continue
                if parsed.timestamp and parsed.timestamp < cutoff_ts:
                    hit_older_row = True
                    continue
                events.append(parsed)
            if len(page) < page_size or hit_older_row:
                break
        return events

    def _load_wallet_activity_events(self, wallet: str, cutoff_ts: int) -> list[WalletTradeEvent]:
        events: list[WalletTradeEvent] = []
        page_size = min(500, max(1, int(self.signal_page_size)))
        max_pages = max(1, int(self.signal_max_pages))
        for page_index in range(max_pages):
            page = self.client.get_user_activity(
                wallet,
                limit=page_size,
                offset=page_index * page_size,
                start_ts=cutoff_ts,
            )
            if not page:
                break
            for row in page:
                parsed = self._activity_to_event(row)
                if parsed is None:
                    continue
                if parsed.timestamp and parsed.timestamp < cutoff_ts:
                    continue
                events.append(parsed)
            if len(page) < page_size:
                break
        return events

    def _record_wallet_seen_token(self, wallet: str, token_id: str) -> None:
        wallet_key = str(wallet or "").strip().lower()
        token_key = str(token_id or "").strip()
        if not wallet_key or not token_key:
            return
        self._wallet_seen_tokens.setdefault(wallet_key, set()).add(token_key)

    def _wallet_has_seen_token(self, wallet: str, token_id: str) -> bool:
        wallet_key = str(wallet or "").strip().lower()
        token_key = str(token_id or "").strip()
        if not wallet_key or not token_key:
            return False
        return token_key in self._wallet_seen_tokens.get(wallet_key, set())

    def detect_wallet_events(self, wallet: str, *, warmup: bool = False) -> list[RawWalletEvent]:
        if not self._event_signal_enabled():
            return []

        mode = self._normalized_signal_source()
        cutoff_ts = self._event_cutoff_ts(wallet)
        combined: list[RawWalletEvent] = []

        if mode in {"trades", "hybrid"}:
            try:
                combined.extend(self._load_wallet_trade_events(wallet, cutoff_ts))
            except Exception:
                if mode == "trades":
                    return []
        if mode in {"activity", "hybrid"}:
            try:
                combined.extend(self._load_wallet_activity_events(wallet, cutoff_ts))
            except Exception:
                if mode == "activity" and not combined:
                    return []

        deduped: dict[str, WalletTradeEvent] = {}
        for event in sorted(combined, key=lambda row: (row.timestamp, row.source, row.dedupe_key)):
            deduped.setdefault(event.dedupe_key, event)
        events = list(deduped.values())

        if warmup and wallet not in self._wallet_event_watermarks:
            self._prime_wallet_event_cursor(wallet)
            return []

        return self._update_wallet_event_cursor(wallet, events)

    def _recent_wallet_events(self, wallet: str, *, warmup: bool) -> list[RawWalletEvent]:
        return self.detect_wallet_events(wallet, warmup=warmup)

    @staticmethod
    def _buy_action_and_label(trigger_type: str, *, event_driven: bool) -> tuple[str, str]:
        normalized = str(trigger_type or "").strip().lower()
        if normalized == "readd":
            return "entry", "回补买入" if event_driven else "回补买入"
        if normalized == "add":
            return "add", "事件加仓" if event_driven else "追加买入"
        if normalized == "multi_wallet_confirm":
            return "entry", "共振买入"
        return "entry", "事件入场" if event_driven else "首次入场"

    @staticmethod
    def _exit_action_and_label(*, event_driven: bool, exit_fraction: float) -> tuple[str, str]:
        if exit_fraction >= 0.95:
            return "exit", "事件退出" if event_driven else "完全退出"
        return "trim", "事件减仓" if event_driven else "部分减仓"

    def _classify_buy_trigger(
        self,
        *,
        wallet: str,
        token_id: str,
        prev: PositionState | None,
        current: Position | None,
        event_driven: bool,
    ) -> str:
        if prev is not None:
            if current is not None and current.notional > prev.notional + 1e-9:
                return "add"
            if event_driven and current is None:
                return "readd" if self._wallet_has_seen_token(wallet, token_id) else "new_open"
            return "add"
        if self._wallet_has_seen_token(wallet, token_id):
            return "readd"
        return "new_open"

    def _topic_context(
        self,
        market_slug: str,
        topic_profiles: list[dict[str, object]],
    ) -> tuple[str, str, dict[str, object] | None, str, int, float, float, float]:
        topic_key, topic_label, topic_profile = self._topic_profile_for_market(market_slug, topic_profiles)
        topic_summary = self._topic_summary(topic_label, topic_profile)
        topic_sample_count = int((topic_profile or {}).get("sample_count") or 0)
        topic_win_rate = float((topic_profile or {}).get("win_rate") or 0.0)
        topic_roi = float((topic_profile or {}).get("roi") or 0.0)
        topic_resolved_win_rate = float((topic_profile or {}).get("resolved_win_rate") or 0.0)
        return (
            topic_key,
            topic_label,
            topic_profile,
            topic_summary,
            topic_sample_count,
            topic_win_rate,
            topic_roi,
            topic_resolved_win_rate,
        )

    def _build_candidate_context(
        self,
        *,
        wallet: str,
        token_id: str,
        condition_id: str,
        market_slug: str,
        outcome: str,
        side: str,
        trigger_type: str,
        confidence: float,
        price_hint: float,
        observed_size: float,
        observed_notional: float,
        timestamp: int,
        wallet_score: float,
        wallet_tier: str,
        wallet_score_summary: str,
        topic_key: str,
        topic_label: str,
        topic_sample_count: int,
        topic_win_rate: float,
        topic_roi: float,
        topic_resolved_win_rate: float,
        topic_score_summary: str,
        position_action: str,
        position_action_label: str,
        exit_fraction: float = 0.0,
        exit_reason: str = "",
        cross_wallet_exit: bool = False,
        source_event: RawWalletEvent | None = None,
        previous_position: PositionState | None = None,
        current_position: Position | None = None,
    ) -> WalletCandidateContext:
        return WalletCandidateContext(
            wallet=wallet,
            token_id=token_id,
            condition_id=condition_id,
            market_slug=market_slug,
            outcome=outcome,
            side=side,
            trigger_type=trigger_type,
            confidence=confidence,
            price_hint=price_hint,
            observed_size=observed_size,
            observed_notional=observed_notional,
            timestamp=self._event_timestamp(timestamp),
            wallet_score=wallet_score,
            wallet_tier=wallet_tier,
            wallet_score_summary=wallet_score_summary,
            topic_key=topic_key,
            topic_label=topic_label,
            topic_sample_count=topic_sample_count,
            topic_win_rate=topic_win_rate,
            topic_roi=topic_roi,
            topic_resolved_win_rate=topic_resolved_win_rate,
            topic_score_summary=topic_score_summary,
            position_action=position_action,
            position_action_label=position_action_label,
            exit_fraction=max(0.0, min(1.0, exit_fraction)),
            exit_reason=exit_reason,
            cross_wallet_exit=cross_wallet_exit,
            source_event=source_event,
            previous_position=previous_position,
            current_position=current_position,
        )

    def build_candidates(
        self,
        wallet: str,
        latest_positions: list[Position],
        wallet_state: dict[str, PositionState],
        wallet_metrics: Mapping[str, object],
        raw_events: list[RawWalletEvent],
        *,
        wallet_is_eligible: bool,
        is_warmup_cycle: bool,
    ) -> list[WalletCandidateContext]:
        candidates: list[WalletCandidateContext] = []
        latest_by_token = {pos.token_id: pos for pos in latest_positions}
        wallet_score = float(wallet_metrics.get("wallet_score") or 0.0)
        wallet_tier = str(wallet_metrics.get("wallet_tier") or "LOW")
        wallet_score_summary = str(wallet_metrics.get("score_summary") or "")
        topic_profiles = list(wallet_metrics.get("topic_profiles") or [])
        emitted_event_sides: set[tuple[str, str]] = set()
        seen_tokens: set[str] = set()

        for event in raw_events:
            token = event.token_id
            prev = wallet_state.get(token)
            current = latest_by_token.get(token)
            was_seen_before = self._wallet_has_seen_token(wallet, token)
            topic_key, topic_label, topic_profile, topic_summary, topic_sample_count, topic_win_rate, topic_roi, topic_resolved_win_rate = self._topic_context(
                event.market_slug or (current.market_slug if current is not None else (prev.market_slug if prev is not None else token)),
                topic_profiles,
            )

            if event.side == "BUY":
                if (not wallet_is_eligible) or event.notional < self.min_increase_usd:
                    self._record_wallet_seen_token(wallet, token)
                    continue
                trigger_type = "readd" if was_seen_before else "new_open"
                if prev is not None:
                    trigger_type = "add" if current is not None or prev is not None else trigger_type
                action, action_label = self._buy_action_and_label(trigger_type, event_driven=True)
                confidence = 0.75 if prev is None else min(0.95, 0.65 + event.notional / 5000.0)
                candidates.append(
                    self._build_candidate_context(
                        wallet=wallet,
                        token_id=token,
                        condition_id=(
                            event.condition_id
                            or (current.condition_id if current is not None else "")
                            or (prev.condition_id if prev is not None else "")
                        ),
                        market_slug=event.market_slug or (current.market_slug if current is not None else token),
                        outcome=event.outcome or (current.outcome if current is not None else (prev.outcome if prev is not None else "YES")),
                        side="BUY",
                        trigger_type=trigger_type,
                        confidence=confidence,
                        price_hint=max(
                            0.01,
                            event.price
                            or (current.avg_price if current is not None else 0.0)
                            or (prev.price if prev is not None else 0.0),
                        ),
                        observed_size=event.size,
                        observed_notional=event.notional,
                        timestamp=event.timestamp,
                        wallet_score=wallet_score,
                        wallet_tier=wallet_tier,
                        wallet_score_summary=wallet_score_summary,
                        topic_key=topic_key,
                        topic_label=topic_label,
                        topic_sample_count=topic_sample_count,
                        topic_win_rate=topic_win_rate,
                        topic_roi=topic_roi,
                        topic_resolved_win_rate=topic_resolved_win_rate,
                        topic_score_summary=topic_summary,
                        position_action=action,
                        position_action_label=action_label,
                        source_event=event,
                        previous_position=prev,
                        current_position=current,
                    )
                )
                emitted_event_sides.add((token, "BUY"))
                self._record_wallet_seen_token(wallet, token)
                continue

            if (not self.follow_wallet_exits) or prev is None:
                self._record_wallet_seen_token(wallet, token)
                continue
            reduction = max(0.0, event.notional)
            if reduction < self.min_decrease_usd and current is not None:
                self._record_wallet_seen_token(wallet, token)
                continue
            if current is None:
                exit_fraction = 1.0
                exit_reason = f"source wallet fully exited via {event.source}"
            else:
                exit_fraction = min(1.0, reduction / max(0.01, prev.notional))
                exit_reason = f"source wallet trimmed via {event.source} | delta ${reduction:.0f}"
            action, action_label = self._exit_action_and_label(event_driven=True, exit_fraction=exit_fraction)
            confidence = min(0.95, 0.55 + exit_fraction * 0.35)
            candidates.append(
                self._build_candidate_context(
                    wallet=wallet,
                    token_id=token,
                    condition_id=(
                        event.condition_id
                        or (current.condition_id if current is not None else "")
                        or prev.condition_id
                    ),
                    market_slug=event.market_slug or (current.market_slug if current is not None else prev.market_slug or token),
                    outcome=event.outcome or (current.outcome if current is not None else prev.outcome or "YES"),
                    side="SELL",
                    trigger_type="exit",
                    confidence=confidence,
                    price_hint=max(
                        0.01,
                        event.price
                        or (current.avg_price if current is not None else 0.0)
                        or prev.price,
                    ),
                    observed_size=event.size,
                    observed_notional=max(reduction, prev.notional if current is None else reduction),
                    timestamp=event.timestamp,
                    wallet_score=wallet_score,
                    wallet_tier=wallet_tier,
                    wallet_score_summary=wallet_score_summary,
                    topic_key=topic_key,
                    topic_label=topic_label,
                    topic_sample_count=topic_sample_count,
                    topic_win_rate=topic_win_rate,
                    topic_roi=topic_roi,
                    topic_resolved_win_rate=topic_resolved_win_rate,
                    topic_score_summary=topic_summary,
                    position_action=action,
                    position_action_label=action_label,
                    exit_fraction=exit_fraction,
                    exit_reason=exit_reason,
                    source_event=event,
                    previous_position=prev,
                    current_position=current,
                )
            )
            emitted_event_sides.add((token, "SELL"))
            self._record_wallet_seen_token(wallet, token)

        for pos in latest_positions:
            token = pos.token_id
            seen_tokens.add(token)
            prev = wallet_state.get(token)
            was_seen_before = self._wallet_has_seen_token(wallet, token)
            self._record_wallet_seen_token(wallet, token)
            topic_key, topic_label, topic_profile, topic_summary, topic_sample_count, topic_win_rate, topic_roi, topic_resolved_win_rate = self._topic_context(
                pos.market_slug or token,
                topic_profiles,
            )

            if prev is None:
                if (
                    wallet_is_eligible
                    and (not is_warmup_cycle)
                    and pos.notional >= self.min_increase_usd
                    and (token, "BUY") not in emitted_event_sides
                ):
                    trigger_type = "readd" if was_seen_before else "new_open"
                    action, action_label = self._buy_action_and_label(trigger_type, event_driven=False)
                    candidates.append(
                        self._build_candidate_context(
                            wallet=wallet,
                            token_id=token,
                            condition_id=str(pos.condition_id or ""),
                            market_slug=pos.market_slug,
                            outcome=pos.outcome,
                            side="BUY",
                            trigger_type=trigger_type,
                            confidence=0.75,
                            price_hint=pos.avg_price,
                            observed_size=pos.size,
                            observed_notional=pos.notional,
                            timestamp=pos.timestamp,
                            wallet_score=wallet_score,
                            wallet_tier=wallet_tier,
                            wallet_score_summary=wallet_score_summary,
                            topic_key=topic_key,
                            topic_label=topic_label,
                            topic_sample_count=topic_sample_count,
                            topic_win_rate=topic_win_rate,
                            topic_roi=topic_roi,
                            topic_resolved_win_rate=topic_resolved_win_rate,
                            topic_score_summary=topic_summary,
                            position_action=action,
                            position_action_label=action_label,
                            previous_position=prev,
                            current_position=pos,
                        )
                    )
            else:
                delta = pos.notional - prev.notional
                if wallet_is_eligible and delta >= self.min_increase_usd and (token, "BUY") not in emitted_event_sides:
                    trigger_type = "add"
                    action, action_label = self._buy_action_and_label(trigger_type, event_driven=False)
                    conf = min(0.95, 0.65 + delta / 5000.0)
                    candidates.append(
                        self._build_candidate_context(
                            wallet=wallet,
                            token_id=token,
                            condition_id=str(pos.condition_id or prev.condition_id),
                            market_slug=pos.market_slug,
                            outcome=pos.outcome,
                            side="BUY",
                            trigger_type=trigger_type,
                            confidence=conf,
                            price_hint=pos.avg_price,
                            observed_size=max(0.0, pos.size - prev.size),
                            observed_notional=delta,
                            timestamp=pos.timestamp,
                            wallet_score=wallet_score,
                            wallet_tier=wallet_tier,
                            wallet_score_summary=wallet_score_summary,
                            topic_key=topic_key,
                            topic_label=topic_label,
                            topic_sample_count=topic_sample_count,
                            topic_win_rate=topic_win_rate,
                            topic_roi=topic_roi,
                            topic_resolved_win_rate=topic_resolved_win_rate,
                            topic_score_summary=topic_summary,
                            position_action=action,
                            position_action_label=action_label,
                            previous_position=prev,
                            current_position=pos,
                        )
                    )
                elif (
                    self.follow_wallet_exits
                    and delta <= -self.min_decrease_usd
                    and (token, "SELL") not in emitted_event_sides
                ):
                    reduction = abs(delta)
                    exit_fraction = min(1.0, reduction / max(0.01, prev.notional))
                    conf = min(0.95, 0.55 + exit_fraction * 0.35)
                    action, action_label = self._exit_action_and_label(event_driven=False, exit_fraction=exit_fraction)
                    candidates.append(
                        self._build_candidate_context(
                            wallet=wallet,
                            token_id=token,
                            condition_id=str(pos.condition_id or prev.condition_id),
                            market_slug=pos.market_slug,
                            outcome=pos.outcome,
                            side="SELL",
                            trigger_type="exit",
                            confidence=conf,
                            price_hint=max(0.01, pos.avg_price or prev.price),
                            observed_size=max(0.0, prev.size - pos.size),
                            observed_notional=reduction,
                            timestamp=pos.timestamp,
                            wallet_score=wallet_score,
                            wallet_tier=wallet_tier,
                            wallet_score_summary=wallet_score_summary,
                            topic_key=topic_key,
                            topic_label=topic_label,
                            topic_sample_count=topic_sample_count,
                            topic_win_rate=topic_win_rate,
                            topic_roi=topic_roi,
                            topic_resolved_win_rate=topic_resolved_win_rate,
                            topic_score_summary=topic_summary,
                            position_action=action,
                            position_action_label=action_label,
                            exit_fraction=exit_fraction,
                            exit_reason=(
                                f"source wallet trimmed {exit_fraction:.0%}"
                                f" | delta ${reduction:.0f}"
                            ),
                            previous_position=prev,
                            current_position=pos,
                        )
                    )

            wallet_state[token] = PositionState(
                size=pos.size,
                notional=pos.notional,
                price=pos.avg_price,
                updated_ts=pos.timestamp,
                condition_id=str(pos.condition_id or ""),
                market_slug=pos.market_slug,
                outcome=pos.outcome,
            )

        for token in list(wallet_state.keys()):
            if token in seen_tokens:
                continue
            prev = wallet_state[token]
            self._record_wallet_seen_token(wallet, token)
            if (
                self.follow_wallet_exits
                and (not is_warmup_cycle)
                and prev.notional >= self.min_decrease_usd
                and (token, "SELL") not in emitted_event_sides
            ):
                action, action_label = self._exit_action_and_label(event_driven=False, exit_fraction=1.0)
                candidates.append(
                    self._build_candidate_context(
                        wallet=wallet,
                        token_id=token,
                        condition_id=str(prev.condition_id or ""),
                        market_slug=prev.market_slug or token,
                        outcome=prev.outcome or "YES",
                        side="SELL",
                        trigger_type="exit",
                        confidence=0.9,
                        price_hint=max(0.01, prev.price),
                        observed_size=prev.size,
                        observed_notional=prev.notional,
                        timestamp=wallet_metrics.get("history_refresh_ts", 0) if isinstance(wallet_metrics, Mapping) else 0,
                        wallet_score=wallet_score,
                        wallet_tier=wallet_tier,
                        wallet_score_summary=wallet_score_summary,
                        topic_key="",
                        topic_label="",
                        topic_sample_count=0,
                        topic_win_rate=0.0,
                        topic_roi=0.0,
                        topic_resolved_win_rate=0.0,
                        topic_score_summary="",
                        position_action=action,
                        position_action_label=action_label,
                        exit_fraction=1.0,
                        exit_reason="source wallet fully exited",
                        previous_position=prev,
                    )
                )
            del wallet_state[token]

        return candidates

    def _candidate_to_signal(self, candidate: WalletCandidateContext) -> Signal:
        return Signal(
            signal_id="",
            trace_id="",
            wallet=candidate.wallet,
            market_slug=candidate.market_slug,
            token_id=candidate.token_id,
            condition_id=candidate.condition_id,
            outcome=candidate.outcome,
            side=candidate.side,  # type: ignore[arg-type]
            confidence=candidate.confidence,
            price_hint=max(0.01, candidate.price_hint),
            observed_size=candidate.observed_size,
            observed_notional=candidate.observed_notional,
            timestamp=candidate.timestamp,
            wallet_score=candidate.wallet_score,
            wallet_tier=candidate.wallet_tier,
            wallet_score_summary=candidate.wallet_score_summary,
            topic_key=candidate.topic_key,
            topic_label=candidate.topic_label,
            topic_sample_count=candidate.topic_sample_count,
            topic_win_rate=candidate.topic_win_rate,
            topic_roi=candidate.topic_roi,
            topic_resolved_win_rate=candidate.topic_resolved_win_rate,
            topic_score_summary=candidate.topic_score_summary,
            exit_fraction=candidate.exit_fraction,
            exit_reason=candidate.exit_reason,
            cross_wallet_exit=candidate.cross_wallet_exit,
            position_action=candidate.position_action,
            position_action_label=candidate.position_action_label,
        )

    def _boost_buy_resonance(
        self,
        candidates: list[WalletCandidateContext],
    ) -> WalletCandidateContext:
        ordered = sorted(
            candidates,
            key=lambda row: (row.wallet_score, row.confidence, row.observed_notional),
            reverse=True,
        )
        representative = ordered[0]
        wallets = tuple(
            wallet
            for wallet in dict.fromkeys(str(row.wallet or "").strip().lower() for row in ordered)
            if wallet
        )
        wallet_count = len(wallets)
        if wallet_count <= 1:
            return representative

        boost = min(0.12, 0.04 * (wallet_count - 1) + min(0.04, sum(row.wallet_score for row in ordered) / 5000.0))
        confidence = min(0.95, representative.confidence + boost)
        observed_notional = sum(row.observed_notional for row in ordered)
        observed_size = sum(row.observed_size for row in ordered)
        weighted_price_base = sum(max(0.0, row.observed_notional) * max(0.01, row.price_hint) for row in ordered)
        price_hint = (
            weighted_price_base / observed_notional
            if observed_notional > 0.0
            else max(0.01, representative.price_hint)
        )
        timestamp = max((row.timestamp for row in ordered), default=representative.timestamp)
        wallet_score = max(row.wallet_score for row in ordered)
        wallet_tier = representative.wallet_tier
        summary = f"resonance {wallet_count} wallets"
        if wallets:
            summary = f"{summary} | {', '.join(wallets[:3])}"

        return WalletCandidateContext(
            wallet="wallet-resonance",
            token_id=representative.token_id,
            condition_id=representative.condition_id,
            market_slug=representative.market_slug,
            outcome=representative.outcome,
            side="BUY",
            trigger_type="multi_wallet_confirm",
            confidence=confidence,
            price_hint=price_hint,
            observed_size=observed_size,
            observed_notional=observed_notional,
            timestamp=timestamp,
            wallet_score=wallet_score,
            wallet_tier=wallet_tier,
            wallet_score_summary=summary,
            topic_key=representative.topic_key,
            topic_label=representative.topic_label,
            topic_sample_count=representative.topic_sample_count,
            topic_win_rate=representative.topic_win_rate,
            topic_roi=representative.topic_roi,
            topic_resolved_win_rate=representative.topic_resolved_win_rate,
            topic_score_summary=representative.topic_score_summary,
            position_action="entry",
            position_action_label="共振买入",
            source_event=representative.source_event,
            source_wallet_count=wallet_count,
            resonance_wallets=wallets,
            resonance_boost=boost,
        )

    def rank_candidates(self, candidates: list[WalletCandidateContext]) -> list[Signal]:
        if not candidates:
            return []

        buy_groups: dict[str, list[WalletCandidateContext]] = {}
        sell_signals: list[Signal] = []
        sell_resonance_candidates: dict[str, list[Signal]] = {}

        for candidate in candidates:
            signal = self._candidate_to_signal(candidate)
            if candidate.side == "BUY":
                buy_groups.setdefault(candidate.token_id, []).append(candidate)
                continue
            sell_signals.append(signal)
            sell_resonance_candidates.setdefault(candidate.token_id, []).append(signal)

        signals: list[Signal] = []
        for token_id, rows in buy_groups.items():
            by_wallet: dict[str, WalletCandidateContext] = {}
            for row in sorted(rows, key=lambda item: (item.wallet_score, item.confidence, item.observed_notional), reverse=True):
                wallet = str(row.wallet or "").strip().lower()
                if not wallet:
                    continue
                if wallet not in by_wallet:
                    by_wallet[wallet] = row
            deduped_rows = list(by_wallet.values())
            if not deduped_rows:
                continue
            if len(deduped_rows) > 1:
                boosted = self._boost_buy_resonance(deduped_rows)
                signals.append(self._candidate_to_signal(boosted))
                continue
            signals.append(self._candidate_to_signal(deduped_rows[0]))

        signals.extend(sell_signals)
        signals.extend(self._build_resonance_exit_signals(sell_resonance_candidates))
        signals.sort(
            key=lambda s: (
                1 if s.side == "SELL" else 0,
                1 if s.side == "SELL" and not s.cross_wallet_exit else 0,
                s.wallet_score,
                s.confidence,
                s.observed_notional,
            ),
            reverse=True,
        )
        return signals

    @staticmethod
    def _market_window_bounds(market_slug: str) -> tuple[int | None, int | None, int | None]:
        normalized = str(market_slug or "").strip().lower()
        if not normalized:
            return None, None, None
        match = _MARKET_WINDOW_PATTERN.search(normalized)
        if match is None:
            return None, None, None
        duration_seconds = _MARKET_WINDOW_SECONDS.get(str(match.group(1) or "").strip().lower())
        start_ts = int(match.group(2) or 0)
        if duration_seconds is None or start_ts <= 0:
            return None, None, None
        return start_ts, duration_seconds, start_ts + duration_seconds

    @staticmethod
    def _market_metadata_cache_key(*, condition_id: str = "", market_slug: str = "") -> str:
        normalized_condition = str(condition_id or "").strip().lower()
        if normalized_condition:
            return f"condition:{normalized_condition}"
        normalized_slug = str(market_slug or "").strip().lower()
        if normalized_slug:
            return f"slug:{normalized_slug}"
        return ""

    def _market_metadata(self, *, condition_id: str = "", market_slug: str = "") -> MarketMetadata | None:
        cache_key = self._market_metadata_cache_key(condition_id=condition_id, market_slug=market_slug)
        if not cache_key:
            return None
        now_ts = int(time.time())
        cached = self._market_metadata_cache.get(cache_key)
        if cached is not None:
            fetched_ts, metadata = cached
            if fetched_ts > 0 and (now_ts - int(fetched_ts)) <= _MARKET_METADATA_CACHE_TTL_SECONDS:
                return metadata
        getter = getattr(self.client, "get_market_metadata", None)
        metadata: MarketMetadata | None = None
        if callable(getter):
            try:
                metadata = getter(str(condition_id or "").strip(), slug=str(market_slug or "").strip() or None)
            except Exception:
                metadata = None
        self._market_metadata_cache[cache_key] = (now_ts, metadata)
        if metadata is not None:
            if metadata.condition_id:
                self._market_metadata_cache[self._market_metadata_cache_key(condition_id=metadata.condition_id)] = (now_ts, metadata)
            if metadata.market_slug:
                self._market_metadata_cache[self._market_metadata_cache_key(market_slug=metadata.market_slug)] = (now_ts, metadata)
        return metadata

    def _live_market_snapshot(self, token_id: str) -> tuple[float, float] | None:
        token_key = str(token_id or "").strip()
        if not token_key:
            return 0.0, 0.0
        get_order_book = getattr(self.client, "get_order_book", None)
        if not callable(get_order_book):
            return None

        now_ts = int(time.time())
        cached = self._live_market_cache.get(token_key)
        if cached is not None:
            checked_ts, best_bid, best_ask = cached
            if checked_ts > 0 and (now_ts - int(checked_ts)) <= 60:
                return float(best_bid), float(best_ask)

        best_bid = 0.0
        best_ask = 0.0
        try:
            book = get_order_book(token_key)
            if book is not None:
                best_bid = float(getattr(book, "best_bid", 0.0) or 0.0)
                best_ask = float(getattr(book, "best_ask", 0.0) or 0.0)
        except Exception:
            best_bid = 0.0
            best_ask = 0.0
        self._live_market_cache[token_key] = (now_ts, best_bid, best_ask)
        return best_bid, best_ask

    def _token_has_live_orderbook(self, token_id: str) -> bool:
        snapshot = self._live_market_snapshot(token_id)
        if snapshot is None:
            return True
        best_bid, best_ask = snapshot
        return best_bid > 0.0 or best_ask > 0.0

    def _should_skip_short_window_buy(self, signal: Signal) -> bool:
        if signal.side != "BUY":
            return False
        snapshot = self._live_market_snapshot(signal.token_id)
        metadata = self._market_metadata(condition_id=str(signal.condition_id or ""), market_slug=str(signal.market_slug or ""))
        best_ask = 0.0
        if snapshot is not None:
            _, best_ask = snapshot
            if best_ask > 0.0 and signal.price_hint > 0.0:
                chase_pct = ((best_ask - float(signal.price_hint)) / float(signal.price_hint)) * 100.0
                if chase_pct >= float(self.live_buy_max_chase_pct):
                    return True

        if metadata is not None:
            if metadata.closed:
                return True
            if metadata.active is False:
                return True
            if metadata.accepting_orders is False:
                return True
            if metadata.end_ts is not None and int(metadata.end_ts) <= int(time.time()):
                return True

        _, market_window_seconds, market_end_ts = self._market_window_bounds(signal.market_slug)
        if market_window_seconds is None or market_end_ts is None or market_window_seconds > 900:
            return False

        now_ts = int(time.time())
        remaining_seconds = market_end_ts - now_ts
        if remaining_seconds <= 0:
            return True

        if best_ask >= _SHORT_WINDOW_EDGE_ASK:
            return True
        if best_ask > 0.0 and signal.price_hint > 0.0:
            chase_pct = ((best_ask - float(signal.price_hint)) / float(signal.price_hint)) * 100.0
            max_short_window_chase = 3.0 if market_window_seconds <= 300 else 4.0
            if chase_pct >= max_short_window_chase:
                return True

        late_threshold = 60 if market_window_seconds <= 300 else 90
        return remaining_seconds <= late_threshold

    def _select_live_signals(self, ranked_signals: list[Signal]) -> list[Signal]:
        selected: list[Signal] = []
        for signal in ranked_signals:
            if signal.side == "BUY":
                if not self._token_has_live_orderbook(signal.token_id):
                    continue
                if self._should_skip_short_window_buy(signal):
                    continue
            selected.append(signal)
            if len(selected) >= self.max_signals_per_cycle:
                break
        return selected

    def generate_signals(self, wallets: list[str]) -> list[Signal]:
        self._live_market_cache.clear()
        prior_wallet_metrics = {wallet: dict(metrics) for wallet, metrics in self._last_wallet_metrics.items()}
        wallets_to_track = list(dict.fromkeys(wallets + list(self._state.keys())))
        eligible_wallets = self._screen_wallets(wallets_to_track)

        all_candidates: list[WalletCandidateContext] = []
        for wallet in wallets_to_track:
            latest_positions = self._latest_wallet_positions.get(wallet, [])
            wallet_state = self._state.setdefault(wallet, {})
            is_warmup_cycle = len(wallet_state) == 0
            wallet_metrics = self._last_wallet_metrics.get(wallet) or prior_wallet_metrics.get(wallet, {})
            raw_events = self.detect_wallet_events(wallet, warmup=is_warmup_cycle)
            all_candidates.extend(
                self.build_candidates(
                    wallet,
                    latest_positions,
                    wallet_state,
                    wallet_metrics,
                    raw_events,
                    wallet_is_eligible=wallet in eligible_wallets,
                    is_warmup_cycle=is_warmup_cycle,
                )
            )

        ranked_signals = self.rank_candidates(all_candidates)
        return self._select_live_signals(ranked_signals)

    def _build_resonance_exit_signals(self, candidates: Mapping[str, list[Signal]]) -> list[Signal]:
        if not self.resonance_exit_enabled:
            return []

        signals: list[Signal] = []
        for token, rows in candidates.items():
            unique_rows: list[Signal] = []
            seen_wallets: set[str] = set()
            for row in sorted(rows, key=lambda signal: (signal.wallet_score, signal.confidence), reverse=True):
                wallet = str(row.wallet or "").strip().lower()
                if not wallet or wallet in seen_wallets:
                    continue
                seen_wallets.add(wallet)
                if float(row.wallet_score or 0.0) < float(self.resonance_min_wallet_score):
                    continue
                unique_rows.append(row)

            if len(unique_rows) < int(self.resonance_min_wallets):
                continue

            representative = unique_rows[0]
            fully_exited_core = [
                row
                for row in unique_rows
                if float(row.exit_fraction or 0.0) >= 0.95 and str(row.wallet_tier or "").upper() == "CORE"
            ]
            if fully_exited_core:
                exit_fraction = float(self.resonance_core_exit_fraction)
                exit_reason = (
                    f"multi-wallet exit resonance | {len(unique_rows)} wallets"
                    f" | {len(fully_exited_core)} CORE full exit"
                )
            else:
                exit_fraction = float(self.resonance_trim_fraction)
                exit_reason = f"multi-wallet exit resonance | {len(unique_rows)} wallets trimming"

            signals.append(
                Signal(
                    signal_id="",
                    trace_id="",
                    wallet="wallet-resonance",
                    market_slug=representative.market_slug,
                    token_id=token,
                    condition_id=str(representative.condition_id or ""),
                    outcome=representative.outcome,
                    side="SELL",
                    confidence=min(0.95, 0.62 + 0.08 * len(unique_rows)),
                    price_hint=max(0.01, representative.price_hint),
                    observed_size=max(float(row.observed_size or 0.0) for row in unique_rows),
                    observed_notional=max(float(row.observed_notional or 0.0) for row in unique_rows),
                    timestamp=datetime.now(tz=timezone.utc),
                    wallet_score=max(float(row.wallet_score or 0.0) for row in unique_rows),
                    wallet_tier=representative.wallet_tier,
                    wallet_score_summary=f"resonance {len(unique_rows)} wallets",
                    topic_key=representative.topic_key,
                    topic_label=representative.topic_label,
                    topic_sample_count=representative.topic_sample_count,
                    topic_win_rate=representative.topic_win_rate,
                    topic_roi=representative.topic_roi,
                    topic_resolved_win_rate=representative.topic_resolved_win_rate,
                    topic_score_summary=representative.topic_score_summary,
                    exit_fraction=max(0.0, min(1.0, exit_fraction)),
                    exit_reason=exit_reason,
                    cross_wallet_exit=True,
                    exit_wallet_count=len(unique_rows),
                    position_action="trim",
                    position_action_label="共振减仓",
                )
            )
        return signals
