from __future__ import annotations

import json
import logging
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone

from polymarket_bot.brokers.base import Broker
from polymarket_bot.clients.data_api import PolymarketDataClient
from polymarket_bot.config import Settings
from polymarket_bot.risk import RiskManager, RiskState
from polymarket_bot.strategies.wallet_follower import WalletFollowerStrategy
from polymarket_bot.types import Signal


@dataclass(slots=True)
class ControlState:
    pause_opening: bool = False
    reduce_only: bool = False
    emergency_stop: bool = False
    updated_ts: int = 0


@dataclass(slots=True)
class Trader:
    settings: Settings
    data_client: PolymarketDataClient
    strategy: WalletFollowerStrategy
    risk: RiskManager
    broker: Broker
    state: RiskState = field(init=False)
    log: logging.Logger = field(init=False)
    _cached_wallets: list[str] = field(init=False, default_factory=list)
    _cached_wallets_ts: float = field(init=False, default=0.0)
    _wallet_cache_ready: bool = field(init=False, default=False)
    last_wallets: list[str] = field(init=False, default_factory=list)
    last_signals: list[Signal] = field(init=False, default_factory=list)
    recent_orders: deque[dict[str, object]] = field(init=False, default_factory=lambda: deque(maxlen=100))
    positions_book: dict[str, dict[str, object]] = field(init=False, default_factory=dict)
    token_reentry_until: dict[str, int] = field(init=False, default_factory=dict)
    control_state: ControlState = field(init=False, default_factory=ControlState)
    _last_control_signature: tuple[bool, bool, bool, int] = field(
        init=False,
        default=(False, False, False, 0),
    )

    def __post_init__(self) -> None:
        self.state = RiskState()
        self.log = logging.getLogger("polybot")

    def _load_control_state(self) -> ControlState:
        payload: dict[str, object] = {}
        try:
            with open(self.settings.control_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict):
                payload = data
        except FileNotFoundError:
            payload = {}
        except Exception as exc:
            self.log.warning("Control state read failed path=%s err=%s", self.settings.control_path, exc)
            payload = {}

        state = ControlState(
            pause_opening=bool(payload.get("pause_opening", False)),
            reduce_only=bool(payload.get("reduce_only", False)),
            emergency_stop=bool(payload.get("emergency_stop", False)),
            updated_ts=int(payload.get("updated_ts") or 0),
        )

        signature = (
            state.pause_opening,
            state.reduce_only,
            state.emergency_stop,
            state.updated_ts,
        )
        if signature != self._last_control_signature:
            self._last_control_signature = signature
            self.log.info(
                "CONTROL pause_opening=%s reduce_only=%s emergency_stop=%s updated_ts=%d",
                state.pause_opening,
                state.reduce_only,
                state.emergency_stop,
                state.updated_ts,
            )

        self.control_state = state
        return state

    def _apply_emergency_exit(self) -> None:
        if not self.positions_book:
            return

        now = int(time.time())
        close_notional = self.settings.stale_position_close_notional_usd

        for token_id in list(self.positions_book.keys()):
            position = self.positions_book.get(token_id)
            if not position:
                continue

            current_notional = float(position.get("notional") or 0.0)
            current_qty = float(position.get("quantity") or 0.0)
            if current_notional <= 0 or current_qty <= 0:
                del self.positions_book[token_id]
                self.state.open_positions = max(0, self.state.open_positions - 1)
                continue

            sig = Signal(
                wallet="system-emergency-stop",
                market_slug=str(position.get("market_slug") or token_id),
                token_id=token_id,
                outcome=str(position.get("outcome") or "YES"),
                side="SELL",
                confidence=1.0,
                price_hint=float(position.get("price") or 0.5),
                observed_size=current_qty,
                observed_notional=current_notional,
                timestamp=datetime.now(tz=timezone.utc),
            )
            result = self.broker.execute(sig, current_notional)
            if not result.ok:
                self.recent_orders.appendleft(
                    {
                        "ts": now,
                        "title": sig.market_slug,
                        "side": sig.side,
                        "status": "REJECTED",
                        "retry_count": 0,
                        "latency_ms": 0,
                        "reason": f"emergency-exit failed: {result.message}",
                    }
                )
                self.log.error(
                    "EMERGENCY_EXIT_FAIL slug=%s token=%s reason=%s",
                    sig.market_slug,
                    sig.token_id,
                    result.message,
                )
                continue

            filled_qty = result.filled_notional / max(0.01, result.filled_price)
            remaining_notional = max(0.0, current_notional - result.filled_notional)
            remaining_qty = max(0.0, current_qty - filled_qty)
            position["notional"] = remaining_notional
            position["quantity"] = remaining_qty
            position["price"] = result.filled_price
            position["last_trim_ts"] = now

            self.recent_orders.appendleft(
                {
                    "ts": now,
                    "title": sig.market_slug,
                    "side": sig.side,
                    "status": "FILLED",
                    "retry_count": 0,
                    "latency_ms": 0,
                    "reason": "emergency-exit",
                }
            )
            self.log.warning(
                "EMERGENCY_EXIT slug=%s token=%s notional=%.2f remain_notional=%.2f",
                sig.market_slug,
                sig.token_id,
                result.filled_notional,
                remaining_notional,
            )

            if remaining_notional <= close_notional or remaining_qty <= 0:
                del self.positions_book[token_id]
                self.state.open_positions = max(0, self.state.open_positions - 1)
                if self.settings.token_reentry_cooldown_seconds > 0:
                    self.token_reentry_until[token_id] = now + self.settings.token_reentry_cooldown_seconds
                self.log.warning(
                    "EMERGENCY_EXIT_CLOSE slug=%s token=%s open_positions=%d",
                    sig.market_slug,
                    sig.token_id,
                    self.state.open_positions,
                )

    def _resolve_wallets(self) -> list[str]:
        seed_wallets = self.settings.wallet_list
        mode = self.settings.wallet_discovery_mode.strip().lower()
        if not self.settings.wallet_discovery_enabled:
            if mode == "replace":
                return []
            return seed_wallets
        now = time.time()
        cache_age = now - self._cached_wallets_ts
        if self._wallet_cache_ready and cache_age < self.settings.wallet_discovery_refresh_seconds:
            return self._cached_wallets

        try:
            discovered_counts = self.data_client.discover_wallet_activity(
                paths=self.settings.wallet_discovery_path_list,
                limit=self.settings.wallet_discovery_limit,
            )
        except Exception as exc:
            fallback = self._cached_wallets if self._wallet_cache_ready else (seed_wallets if mode != "replace" else [])
            self.log.warning("Wallet discovery failed, fallback wallets=%d: %s", len(fallback), exc)
            self._cached_wallets = fallback
            self._cached_wallets_ts = now
            self._wallet_cache_ready = True
            return fallback
        discovered = [
            wallet
            for wallet, count in sorted(
                discovered_counts.items(),
                key=lambda item: (item[1], item[0]),
                reverse=True,
            )
            if count >= self.settings.wallet_discovery_min_events
        ][: self.settings.wallet_discovery_top_n]

        if mode == "replace":
            selected = discovered
        else:
            selected = list(dict.fromkeys(seed_wallets + discovered))

        self.log.info(
            "Wallet universe resolved mode=%s seed=%d discovered=%d selected=%d",
            mode if mode else "union",
            len(seed_wallets),
            len(discovered),
            len(selected),
        )
        self._cached_wallets = selected
        self._cached_wallets_ts = now
        self._wallet_cache_ready = True
        return selected

    def _apply_time_exit(self) -> None:
        if not self.positions_book:
            return

        now = int(time.time())
        utilization = 0.0
        if self.settings.max_open_positions > 0:
            utilization = self.state.open_positions / self.settings.max_open_positions
        congested = utilization >= self.settings.congested_utilization_threshold

        if congested:
            stale_seconds = self.settings.congested_stale_minutes * 60
            trim_pct = self.settings.congested_trim_pct
        else:
            stale_seconds = self.settings.stale_position_minutes * 60
            trim_pct = self.settings.stale_position_trim_pct
        trim_cooldown = self.settings.stale_position_trim_cooldown_seconds
        close_notional = self.settings.stale_position_close_notional_usd

        for token_id in list(self.positions_book.keys()):
            position = self.positions_book.get(token_id)
            if not position:
                continue

            opened_ts = int(position.get("opened_ts") or now)
            if now - opened_ts < stale_seconds:
                continue

            last_trim_ts = int(position.get("last_trim_ts") or 0)
            if now - last_trim_ts < trim_cooldown:
                continue

            current_notional = float(position.get("notional") or 0.0)
            current_qty = float(position.get("quantity") or 0.0)
            if current_notional <= 0 or current_qty <= 0:
                continue

            trim_notional = current_notional * trim_pct
            if trim_notional < 5:
                continue

            sig = Signal(
                wallet="system-time-exit",
                market_slug=str(position.get("market_slug") or token_id),
                token_id=token_id,
                outcome=str(position.get("outcome") or "YES"),
                side="SELL",
                confidence=0.5,
                price_hint=float(position.get("price") or 0.5),
                observed_size=current_qty * trim_pct,
                observed_notional=trim_notional,
                timestamp=datetime.now(tz=timezone.utc),
            )
            result = self.broker.execute(sig, trim_notional)
            if not result.ok:
                self.recent_orders.appendleft(
                    {
                        "ts": now,
                        "title": sig.market_slug,
                        "side": sig.side,
                        "status": "REJECTED",
                        "retry_count": 0,
                        "latency_ms": 0,
                        "reason": f"time-exit failed: {result.message}",
                    }
                )
                self.log.error("TIME_EXIT_FAIL slug=%s token=%s reason=%s", sig.market_slug, sig.token_id, result.message)
                continue

            filled_qty = result.filled_notional / max(0.01, result.filled_price)
            remaining_notional = max(0.0, current_notional - result.filled_notional)
            remaining_qty = max(0.0, current_qty - filled_qty)
            position["notional"] = remaining_notional
            position["quantity"] = remaining_qty
            position["last_trim_ts"] = now
            position["price"] = result.filled_price

            self.recent_orders.appendleft(
                {
                    "ts": now,
                    "title": sig.market_slug,
                    "side": sig.side,
                    "status": "FILLED",
                    "retry_count": 0,
                    "latency_ms": 0,
                    "reason": "time-exit trim",
                }
            )
            self.log.info(
                "TIME_EXIT mode=%s slug=%s token=%s trim_notional=%.2f remain_notional=%.2f",
                "congested" if congested else "normal",
                sig.market_slug,
                sig.token_id,
                result.filled_notional,
                remaining_notional,
            )

            if remaining_notional <= close_notional or remaining_qty <= 0:
                del self.positions_book[token_id]
                self.state.open_positions = max(0, self.state.open_positions - 1)
                if self.settings.token_reentry_cooldown_seconds > 0:
                    self.token_reentry_until[token_id] = now + self.settings.token_reentry_cooldown_seconds
                self.log.info(
                    "TIME_EXIT_CLOSE slug=%s token=%s open_positions=%d",
                    sig.market_slug,
                    sig.token_id,
                    self.state.open_positions,
                )

    def step(self) -> None:
        control = self._load_control_state()
        if control.emergency_stop:
            self._apply_emergency_exit()
            self.last_signals = []
            self.log.warning(
                "EMERGENCY_STOP active, skip opening logic, open_positions=%d",
                self.state.open_positions,
            )
            return

        self._apply_time_exit()
        wallets = self._resolve_wallets()
        self.last_wallets = wallets
        if not wallets:
            self.log.warning("No wallets configured/resolved. Check WATCH_WALLETS and discovery settings.")
            self.last_signals = []
            return

        signals = self.strategy.generate_signals(wallets)
        self.last_signals = signals
        if not signals:
            self.log.info("No actionable signal this cycle")
            return

        for sig in signals:
            if sig.side == "BUY" and control.pause_opening:
                self.log.info(
                    "SKIP wallet=%s slug=%s reason=pause opening enabled",
                    sig.wallet,
                    sig.market_slug,
                )
                continue
            if sig.side == "BUY" and control.reduce_only:
                self.log.info(
                    "SKIP wallet=%s slug=%s reason=reduce-only mode",
                    sig.wallet,
                    sig.market_slug,
                )
                continue

            now = int(time.time())
            cooldown_until = int(self.token_reentry_until.get(sig.token_id, 0))
            if cooldown_until > 0 and cooldown_until <= now:
                del self.token_reentry_until[sig.token_id]
                cooldown_until = 0
            if (
                sig.side == "BUY"
                and sig.token_id not in self.positions_book
                and cooldown_until > now
            ):
                self.log.info(
                    "SKIP wallet=%s slug=%s reason=token reentry cooldown %ds",
                    sig.wallet,
                    sig.market_slug,
                    cooldown_until - now,
                )
                continue

            existing = self.positions_book.get(sig.token_id)
            if existing is not None and self.settings.token_add_cooldown_seconds > 0:
                last_buy_ts = int(existing.get("last_buy_ts") or existing.get("opened_ts") or 0)
                remain = self.settings.token_add_cooldown_seconds - (now - last_buy_ts)
                if remain > 0:
                    self.log.info(
                        "SKIP wallet=%s slug=%s reason=token add cooldown %ds",
                        sig.wallet,
                        sig.market_slug,
                        remain,
                    )
                    continue

            decision = self.risk.evaluate(sig, self.state)
            if not decision.allowed:
                self.log.info(
                    "SKIP wallet=%s slug=%s reason=%s",
                    sig.wallet,
                    sig.market_slug,
                    decision.reason,
                )
                continue

            result = self.broker.execute(sig, decision.max_notional)
            if result.ok:
                qty = result.filled_notional / max(0.01, result.filled_price)
                now = int(time.time())
                existing = self.positions_book.get(sig.token_id)
                if existing is None:
                    self.state.open_positions += 1
                    self.positions_book[sig.token_id] = {
                        "token_id": sig.token_id,
                        "market_slug": sig.market_slug,
                        "outcome": sig.outcome,
                        "quantity": qty,
                        "price": result.filled_price,
                        "notional": result.filled_notional,
                        "opened_ts": now,
                        "last_buy_ts": now,
                    }
                else:
                    prev_qty = float(existing.get("quantity") or 0.0)
                    prev_notional = float(existing.get("notional") or 0.0)
                    new_qty = prev_qty + qty
                    new_notional = prev_notional + result.filled_notional
                    existing["quantity"] = new_qty
                    existing["notional"] = new_notional
                    existing["price"] = new_notional / max(0.01, new_qty)
                    existing["market_slug"] = sig.market_slug
                    existing["outcome"] = sig.outcome
                    existing["last_buy_ts"] = now
                self.recent_orders.appendleft(
                    {
                        "ts": now,
                        "title": sig.market_slug,
                        "side": sig.side,
                        "status": "FILLED",
                        "retry_count": 0,
                        "latency_ms": 0,
                        "reason": result.message,
                    }
                )
                self.log.info(
                    "EXEC wallet=%s slug=%s token=%s side=%s notional=%.2f px=%.4f order_id=%s msg=%s",
                    sig.wallet,
                    sig.market_slug,
                    sig.token_id,
                    sig.side,
                    result.filled_notional,
                    result.filled_price,
                    result.broker_order_id,
                    result.message,
                )
            else:
                self.recent_orders.appendleft(
                    {
                        "ts": int(time.time()),
                        "title": sig.market_slug,
                        "side": sig.side,
                        "status": "REJECTED",
                        "retry_count": 0,
                        "latency_ms": 0,
                        "reason": result.message,
                    }
                )
                self.log.error(
                    "FAIL wallet=%s slug=%s reason=%s",
                    sig.wallet,
                    sig.market_slug,
                    result.message,
                )

    def run(self, once: bool = False) -> None:
        while True:
            self.step()
            if once:
                return
            time.sleep(self.settings.poll_interval_seconds)
