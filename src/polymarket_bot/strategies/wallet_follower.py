from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone

from polymarket_bot.clients.data_api import PolymarketDataClient, Position
from polymarket_bot.types import Signal


@dataclass(slots=True)
class PositionState:
    size: float
    notional: float
    price: float
    updated_ts: int


@dataclass(slots=True)
class WalletUniverseRule:
    min_active_positions: int
    min_unique_markets: int
    min_total_notional_usd: float
    max_top_market_share: float


@dataclass(slots=True)
class WalletFollowerStrategy:
    client: PolymarketDataClient
    min_increase_usd: float
    max_signals_per_cycle: int
    min_active_positions: int
    min_unique_markets: int
    min_total_notional_usd: float
    max_top_market_share: float
    _state: dict[str, dict[str, PositionState]] = field(default_factory=dict)
    _last_wallet_metrics: dict[str, dict[str, float | int]] = field(default_factory=dict)

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

        for wallet in wallets:
            positions = self.client.get_active_positions(wallet)
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

            selected[wallet] = positions
            self._last_wallet_metrics[wallet] = {
                "positions": active_positions,
                "unique_markets": unique_markets,
                "total_notional": total_notional,
                "top_market_share": top_market_share,
            }

        return selected

    def latest_wallet_metrics(self) -> dict[str, dict[str, float | int]]:
        return self._last_wallet_metrics

    def generate_signals(self, wallets: list[str]) -> list[Signal]:
        signals: list[Signal] = []
        eligible_wallets = self._screen_wallets(wallets)

        for wallet, latest_positions in eligible_wallets.items():
            wallet_state = self._state.setdefault(wallet, {})
            seen_tokens: set[str] = set()
            is_warmup_cycle = len(wallet_state) == 0

            for pos in latest_positions:
                token = pos.token_id
                seen_tokens.add(token)
                prev = wallet_state.get(token)

                if prev is None:
                    if (not is_warmup_cycle) and pos.notional >= self.min_increase_usd:
                        signals.append(
                            Signal(
                                wallet=wallet,
                                market_slug=pos.market_slug,
                                token_id=token,
                                outcome=pos.outcome,
                                side="BUY",
                                confidence=0.75,
                                price_hint=pos.avg_price,
                                observed_size=pos.size,
                                observed_notional=pos.notional,
                                timestamp=datetime.now(tz=timezone.utc),
                            )
                        )
                else:
                    delta = pos.notional - prev.notional
                    if delta >= self.min_increase_usd:
                        conf = min(0.95, 0.65 + delta / 5000.0)
                        signals.append(
                            Signal(
                                wallet=wallet,
                                market_slug=pos.market_slug,
                                token_id=token,
                                outcome=pos.outcome,
                                side="BUY",
                                confidence=conf,
                                price_hint=pos.avg_price,
                                observed_size=max(0.0, pos.size - prev.size),
                                observed_notional=delta,
                                timestamp=datetime.now(tz=timezone.utc),
                            )
                        )

                wallet_state[token] = PositionState(
                    size=pos.size,
                    notional=pos.notional,
                    price=pos.avg_price,
                    updated_ts=pos.timestamp,
                )

            # Keep removed tokens as historical state; they may reappear as fresh signals later.
            _ = seen_tokens

        signals.sort(key=lambda s: (s.confidence, s.observed_notional), reverse=True)
        return signals[: self.max_signals_per_cycle]
