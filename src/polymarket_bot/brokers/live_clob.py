from __future__ import annotations

import json
import logging
import os
import threading
import time
from collections import deque
from datetime import datetime
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from pathlib import Path
from typing import Any, Callable

from polymarket_bot.brokers.base import Broker
from polymarket_bot.clients.data_api import PolymarketDataClient
from polymarket_bot.models import SignerStatusSnapshot
from polymarket_bot.secrets import normalize_identity
from polymarket_bot.signer_client import SignerClient, SignerClientError, SignerHealthSnapshot
from polymarket_bot.types import BrokerOrderEvent, ExecutionResult, OpenOrderSnapshot, OrderFillSnapshot, OrderStatusSnapshot, Signal


def _configure_sdk_http_timeout(timeout_seconds: float | None = None) -> None:
    try:
        import httpx
        from py_clob_client.http_helpers import helpers as sdk_helpers
    except Exception:
        return

    timeout = max(1.0, float(timeout_seconds or os.getenv("POLY_CLOB_HTTP_TIMEOUT_SECONDS", "15") or 15.0))
    current = getattr(sdk_helpers, "_http_client", None)
    current_timeout = getattr(current, "timeout", None)
    if isinstance(current_timeout, httpx.Timeout):
        if all(
            value is not None and float(value) <= timeout
            for value in (
                current_timeout.connect,
                current_timeout.read,
                current_timeout.write,
                current_timeout.pool,
            )
        ):
            return
    try:
        current.close()
    except Exception:
        pass
    sdk_helpers._http_client = httpx.Client(http2=True, timeout=timeout)


class _BufferedUserOrderStream:
    def __init__(
        self,
        *,
        url: str,
        auth: dict[str, str],
        ping_interval_seconds: int,
        reconnect_seconds: int,
        buffer_size: int,
        parser: Callable[[object], list[BrokerOrderEvent]],
        log: logging.Logger,
        websocket_factory: Callable[[str], object] | None = None,
    ) -> None:
        self.url = str(url or "").strip()
        self.auth = {str(key): str(value) for key, value in auth.items() if str(value or "").strip()}
        self.ping_interval_seconds = max(5, int(ping_interval_seconds))
        self.reconnect_seconds = max(1, int(reconnect_seconds))
        self.buffer_size = max(100, int(buffer_size))
        self._parser = parser
        self._log = log
        self._websocket_factory = websocket_factory
        self._events: deque[BrokerOrderEvent] = deque(maxlen=self.buffer_size)
        self._seen_keys: deque[str] = deque()
        self._seen_key_index: set[str] = set()
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._dependency_error: str = ""

    def is_available(self) -> bool:
        return bool(self.url and self.auth)

    def close(self) -> None:
        self._stop_event.set()
        if self._thread is not None and self._thread.is_alive():
            self._thread.join(timeout=2.0)

    def events_since(
        self,
        *,
        since_ts: int = 0,
        order_ids: list[str] | None = None,
        limit: int = 200,
    ) -> list[BrokerOrderEvent] | None:
        if not self.is_available():
            return None
        self._ensure_started()
        if self._dependency_error:
            return None
        normalized_ids = {str(order_id).strip() for order_id in (order_ids or []) if str(order_id).strip()}
        with self._lock:
            rows = [
                event
                for event in self._events
                if (since_ts <= 0 or int(event.timestamp or 0) >= since_ts)
                and (not normalized_ids or str(event.order_id or "").strip() in normalized_ids)
            ]
        rows.sort(key=lambda item: (int(item.timestamp or 0), item.order_id, item.normalized_event_type))
        if limit > 0:
            return rows[-int(limit) :]
        return rows

    def _ensure_started(self) -> None:
        if self._dependency_error:
            return
        if self._thread is not None and self._thread.is_alive():
            return
        if self._stop_event.is_set():
            return
        self._thread = threading.Thread(target=self._run, name="poly-user-stream", daemon=True)
        self._thread.start()

    def _append_events(self, events: list[BrokerOrderEvent]) -> None:
        if not events:
            return
        with self._lock:
            for event in events:
                key = self._event_key(event)
                if key in self._seen_key_index:
                    continue
                self._events.append(event)
                if len(self._seen_keys) >= max(512, self.buffer_size * 4):
                    old = self._seen_keys.popleft()
                    self._seen_key_index.discard(old)
                self._seen_keys.append(key)
                self._seen_key_index.add(key)

    @staticmethod
    def _event_key(event: BrokerOrderEvent) -> str:
        return "|".join(
            [
                str(event.normalized_event_type),
                str(event.order_id or ""),
                str(event.token_id or ""),
                str(int(event.timestamp or 0)),
                str(event.status or ""),
                f"{float(event.matched_notional or 0.0):.10f}",
                f"{float(event.matched_size or 0.0):.10f}",
                f"{float(event.avg_fill_price or 0.0):.10f}",
                str(event.tx_hash or ""),
            ]
        )

    def _open_connection(self) -> tuple[object, type[BaseException]]:
        if callable(self._websocket_factory):
            return self._websocket_factory(self.url), TimeoutError
        try:
            import websocket  # type: ignore[import-not-found]
        except Exception as exc:  # pragma: no cover - dependency optional at runtime
            self._dependency_error = f"websocket-client not installed: {exc}"
            raise RuntimeError(self._dependency_error) from exc
        timeout_exc = getattr(websocket, "WebSocketTimeoutException", TimeoutError)
        return websocket.create_connection(self.url, timeout=5), timeout_exc

    def _send_json(self, ws: object, payload: dict[str, object]) -> None:
        sender = getattr(ws, "send", None)
        if callable(sender):
            sender(json.dumps(payload, ensure_ascii=False))

    def _send_text(self, ws: object, text: str) -> None:
        sender = getattr(ws, "send", None)
        if callable(sender):
            sender(str(text))

    def _recv(self, ws: object) -> object:
        receiver = getattr(ws, "recv", None)
        if not callable(receiver):
            raise RuntimeError("websocket transport missing recv()")
        return receiver()

    def _close_ws(self, ws: object) -> None:
        closer = getattr(ws, "close", None)
        if callable(closer):
            try:
                closer()
            except Exception:
                return

    def _run(self) -> None:  # pragma: no cover - exercised via injected fakes in tests
        while not self._stop_event.is_set():
            ws: object | None = None
            try:
                ws, timeout_exc = self._open_connection()
                settimeout = getattr(ws, "settimeout", None)
                if callable(settimeout):
                    settimeout(1.0)
                self._send_json(
                    ws,
                    {
                        "type": "user",
                        "auth": {
                            "apiKey": self.auth.get("apiKey", ""),
                            "secret": self.auth.get("secret", ""),
                            "passphrase": self.auth.get("passphrase", ""),
                        },
                    },
                )
                last_ping = time.time()
                while not self._stop_event.is_set():
                    now = time.time()
                    if now - last_ping >= self.ping_interval_seconds:
                        self._send_text(ws, "PING")
                        last_ping = now
                    try:
                        raw = self._recv(ws)
                    except timeout_exc:
                        continue
                    if raw in (None, ""):
                        continue
                    if isinstance(raw, bytes):
                        raw = raw.decode("utf-8", errors="ignore")
                    text = str(raw).strip()
                    if not text:
                        continue
                    if text.upper() == "PONG":
                        continue
                    if text.upper() == "PING":
                        self._send_text(ws, "PONG")
                        continue
                    events = self._parser(text)
                    self._append_events(events)
            except Exception as exc:
                if self._dependency_error:
                    self._log.warning("User stream disabled err=%s", self._dependency_error)
                    return
                if not self._stop_event.is_set():
                    self._log.warning("User stream reconnect scheduled err=%s", exc)
                    time.sleep(self.reconnect_seconds)
            finally:
                if ws is not None:
                    self._close_ws(ws)


class _AddressOnlySigner:
    def __init__(self, address: str) -> None:
        self._address = str(address or "").strip().lower()

    def address(self) -> str:
        return self._address


class LiveClobBroker(Broker):
    def __init__(
        self,
        host: str,
        chain_id: int,
        funder: str,
        *,
        signer_client: SignerClient,
        signer_health: SignerHealthSnapshot,
        api_key: str,
        api_secret: str,
        api_passphrase: str,
        market_client: PolymarketDataClient | None = None,
        maker_buffer_ticks: int = 1,
        signature_type: int = 0,
        user_stream_enabled: bool = True,
        user_stream_url: str = "wss://ws-subscriptions-clob.polymarket.com/ws/user",
        user_stream_ping_interval_seconds: int = 10,
        user_stream_reconnect_seconds: int = 5,
        user_stream_buffer_size: int = 1000,
    ) -> None:
        try:
            from py_clob_client.client import ClobClient
            from py_clob_client.clob_types import ApiCreds, OrderArgs, OrderType, PartialCreateOrderOptions
            from py_clob_client.order_builder.constants import BUY, SELL
        except Exception as exc:  # pragma: no cover
            raise RuntimeError(
                "py-clob-client not installed. Install with: pip install '.[live]'"
            ) from exc

        _configure_sdk_http_timeout()

        self._OrderArgs = OrderArgs
        self._OrderType = OrderType
        self._PartialCreateOrderOptions = PartialCreateOrderOptions
        self._side_map = {"BUY": BUY, "SELL": SELL}
        self.market_client = market_client
        self.maker_buffer_ticks = max(0, int(maker_buffer_ticks))
        self._funder = str(funder or "").strip().lower()
        self._log = logging.getLogger("polybot.live_clob")
        self._host = str(host or "").strip()
        self._chain_id = int(chain_id)
        self._signature_type = int(signature_type)
        self._user_stream_enabled = bool(user_stream_enabled)
        self._user_stream_url = str(user_stream_url or "").strip()
        self._signer_client = signer_client
        self._signer_health = signer_health

        if not self._funder:
            raise RuntimeError("funder address missing in live broker")

        api_key_text = str(api_key or "").strip()
        api_secret_text = str(api_secret or "").strip()
        api_passphrase_text = str(api_passphrase or "").strip()
        if not (api_key_text and api_secret_text and api_passphrase_text):
            raise RuntimeError("live api credentials missing")

        self.client = ClobClient(
            host,
            chain_id=chain_id,
            key=None,
            signature_type=signature_type,
            funder=funder,
        )
        creds = ApiCreds(
            api_key=api_key_text,
            api_secret=api_secret_text,
            api_passphrase=api_passphrase_text,
        )
        self.client.set_api_creds(creds)
        self.client.signer = _AddressOnlySigner(self._funder)
        self.client.mode = self.client._get_client_mode()
        self._api_creds = {
            "apiKey": api_key_text,
            "secret": api_secret_text,
            "passphrase": api_passphrase_text,
        }
        self._security_reason_codes: list[str] = []
        self._validate_identity_binding()
        self._signer_security_snapshot = self._build_signer_security_snapshot()
        self._user_stream = (
            _BufferedUserOrderStream(
                url=user_stream_url,
                auth=self._extract_api_creds(self._api_creds),
                ping_interval_seconds=user_stream_ping_interval_seconds,
                reconnect_seconds=user_stream_reconnect_seconds,
                buffer_size=user_stream_buffer_size,
                parser=self._parse_user_stream_message,
                log=self._log,
            )
            if user_stream_enabled
            else None
        )

    def _validate_identity_binding(self) -> None:
        health = self._signer_health
        reason_codes: list[str] = []
        if not bool(health.healthy):
            reason_codes.append(str(health.reason_code or "signer_unhealthy"))
        signer_identity = normalize_identity(health.signer_identity)
        api_identity = normalize_identity(health.api_identity)
        if signer_identity and signer_identity != self._funder:
            reason_codes.append("signer_identity_mismatch")
        if api_identity and api_identity != self._funder:
            reason_codes.append("api_identity_mismatch")
        if not signer_identity:
            reason_codes.append("signer_identity_missing")
        if not api_identity:
            reason_codes.append("api_identity_missing")
        self._security_reason_codes = list(dict.fromkeys(reason_codes))
        if self._security_reason_codes:
            raise RuntimeError(
                "live signer identity binding failed"
            )

    def _build_signer_security_snapshot(self) -> dict[str, object]:
        health = self._signer_health
        signer_identity = normalize_identity(health.signer_identity)
        api_identity = normalize_identity(health.api_identity)
        snapshot = SignerStatusSnapshot(
            live_mode=True,
            signer_required=True,
            signer_mode="external_http",
            signer_healthy=bool(health.healthy),
            signer_identity_matched=bool(signer_identity and signer_identity == self._funder),
            api_identity_matched=bool(api_identity and api_identity == self._funder),
            broker_identity_matched=bool(self._funder),
            raw_key_detected=False,
            funder_identity_present=bool(self._funder),
            api_creds_configured=all(self._extract_api_creds(self._api_creds).values()),
            hot_wallet_cap_enabled=False,
            hot_wallet_cap_ok=True,
            hot_wallet_cap_limit_usd=0.0,
            hot_wallet_cap_value_usd=0.0,
            reason_codes=list(self._security_reason_codes or ([] if health.healthy else [str(health.reason_code or "signer_unhealthy")])),
            last_checked_ts=int(time.time()),
        )
        return snapshot.as_state_payload()

    @staticmethod
    def _env_flag(name: str, default: bool = False) -> bool:
        raw = os.getenv(name)
        if raw is None:
            try:
                env_path = Path(".env")
                if env_path.exists():
                    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
                        line = raw_line.strip()
                        if not line or line.startswith("#") or "=" not in line:
                            continue
                        key, value = line.split("=", 1)
                        if key.strip() == name:
                            raw = value
                            break
            except Exception:
                raw = None
        if raw is None:
            return default
        value = str(raw).strip().lower()
        if not value:
            return default
        if value in {"1", "true", "yes", "y", "on"}:
            return True
        if value in {"0", "false", "no", "n", "off"}:
            return False
        return default

    def startup_checks(self) -> list[dict[str, object]] | None:
        checks: list[dict[str, object]] = []
        checks.append(
            {
                "name": "clob_host",
                "status": "PASS" if self._host else "FAIL",
                "message": self._host or "missing clob host",
            }
        )
        checks.append(
            {
                "name": "funder_address",
                "status": "PASS" if self._funder else "FAIL",
                "message": self._funder or "missing funder address",
            }
        )
        signature_labels = {
            0: "EOA",
            1: "Magic/email wallet",
            2: "proxy/browser wallet",
        }
        checks.append(
            {
                "name": "signature_type",
                "status": "PASS" if self._signature_type in signature_labels else "FAIL",
                "message": f"{self._signature_type} ({signature_labels.get(self._signature_type, 'unknown')})",
            }
        )
        api_creds = self._extract_api_creds(getattr(self, "_api_creds", {}))
        checks.append(
            {
                "name": "api_credentials",
                "status": "PASS" if all(api_creds.values()) else "FAIL",
                "message": "live api creds configured" if all(api_creds.values()) else "missing live api creds",
            }
        )
        signer_snapshot = dict(getattr(self, "_signer_security_snapshot", {}) or {})
        signer_reasons = list(signer_snapshot.get("reason_codes") or [])
        checks.append(
            {
                "name": "signer_health",
                "status": "PASS" if bool(signer_snapshot.get("signer_healthy", False)) else "FAIL",
                "message": "external signer healthy"
                if bool(signer_snapshot.get("signer_healthy", False))
                else ",".join(str(code) for code in signer_reasons) or "signer unhealthy",
            }
        )
        checks.append(
            {
                "name": "signer_identity_binding",
                "status": "PASS"
                if bool(signer_snapshot.get("signer_identity_matched", False))
                and bool(signer_snapshot.get("api_identity_matched", False))
                and bool(signer_snapshot.get("broker_identity_matched", False))
                else "FAIL",
                "message": "signer/api/broker identity matched"
                if bool(signer_snapshot.get("signer_identity_matched", False))
                and bool(signer_snapshot.get("api_identity_matched", False))
                and bool(signer_snapshot.get("broker_identity_matched", False))
                else "identity mismatch detected",
            }
        )
        checks.append(
            {
                "name": "market_preflight",
                "status": "PASS" if self.market_client is not None else "FAIL",
                "message": "market client configured for book/midpoint preflight"
                if self.market_client is not None
                else "market client missing; live preflight will reject orders",
            }
        )
        checks.append(
            {
                "name": "order_status_support",
                "status": "PASS" if callable(getattr(self.client, "get_order", None)) or callable(getattr(self.client, "get_orders", None)) else "FAIL",
                "message": "broker order lookup available"
                if callable(getattr(self.client, "get_order", None)) or callable(getattr(self.client, "get_orders", None))
                else "broker order lookup unavailable",
            }
        )
        checks.append(
            {
                "name": "heartbeat_support",
                "status": "PASS" if callable(getattr(self.client, "heartbeat", None)) else "WARN",
                "message": "heartbeat available for resting orders"
                if callable(getattr(self.client, "heartbeat", None))
                else "heartbeat method unavailable in current SDK surface",
            }
        )
        websocket_ready = False
        websocket_message = "user stream disabled"
        if self._user_stream_enabled:
            try:
                import websocket  # type: ignore[import-not-found]  # noqa: F401

                websocket_ready = True
            except Exception as exc:
                websocket_ready = False
                websocket_message = f"websocket-client missing: {exc}"
            else:
                websocket_message = f"user stream ready at {self._user_stream_url}" if self._user_stream_url else "user stream url missing"
        checks.append(
            {
                "name": "user_stream",
                "status": (
                    "PASS"
                    if self._user_stream_enabled and websocket_ready and self._user_stream_url and all(api_creds.values())
                    else "WARN"
                ),
                "message": websocket_message,
            }
        )
        allowance_ready = self._env_flag("LIVE_ALLOWANCE_READY")
        geoblock_ready = self._env_flag("LIVE_GEOBLOCK_READY")
        account_ready = self._env_flag("LIVE_ACCOUNT_READY")
        prereqs_ready = allowance_ready and geoblock_ready and account_ready
        checks.append(
            {
                "name": "operator_prechecks",
                "status": "PASS" if prereqs_ready else "FAIL",
                "message": (
                    "live admission confirmed"
                    if prereqs_ready
                    else "missing live admission confirmations; set LIVE_ALLOWANCE_READY, LIVE_GEOBLOCK_READY, and LIVE_ACCOUNT_READY to true before live"
                ),
                "details": {
                    "allowance_ready": allowance_ready,
                    "geoblock_ready": geoblock_ready,
                    "account_ready": account_ready,
                },
            }
        )
        return checks

    def security_summary(self) -> dict[str, object] | None:
        return dict(self._signer_security_snapshot or {})

    @staticmethod
    def _safe_float(value: object, default: float = 0.0) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _round_price_to_tick(price: float, tick_size: float, side: str) -> float:
        tick = max(0.0001, float(tick_size))
        direction = ROUND_FLOOR if str(side).upper() == "BUY" else ROUND_CEILING
        price_dec = Decimal(str(max(0.0, price)))
        tick_dec = Decimal(str(tick))
        ticks = (price_dec / tick_dec).to_integral_value(rounding=direction)
        return float(ticks * tick_dec)

    def _preflight_order(
        self,
        signal: Signal,
        notional_usd: float,
    ) -> tuple[float, float, dict[str, float | bool | str], str | None]:
        if self.market_client is None:
            return (0.0, 0.0, {}, "live preflight unavailable: market client not configured")

        try:
            book = self.market_client.get_order_book(signal.token_id)
            midpoint = self.market_client.get_midpoint_price(signal.token_id)
        except Exception as exc:
            return (0.0, 0.0, {}, f"live preflight failed: {exc}")

        if book is None:
            return (0.0, 0.0, {}, "live preflight failed: order book unavailable")

        tick_size = max(0.0001, self._safe_float(book.tick_size, 0.01))
        min_order_size = max(0.0, self._safe_float(book.min_order_size))
        best_bid = max(0.0, self._safe_float(book.best_bid))
        best_ask = max(0.0, self._safe_float(book.best_ask))
        midpoint_value = max(0.0, self._safe_float(midpoint))
        desired_price = max(0.01, min(0.99, signal.price_hint))
        side = str(signal.side).upper()

        adjusted_price = self._round_price_to_tick(desired_price, tick_size, side)
        if midpoint_value > 0.0:
            midpoint_price = self._round_price_to_tick(midpoint_value, tick_size, side)
            if side == "BUY":
                adjusted_price = min(adjusted_price, midpoint_price)
            else:
                adjusted_price = max(adjusted_price, midpoint_price)

        buffer_ticks = self.maker_buffer_ticks or 0
        if side == "BUY" and best_ask > 0.0:
            maker_cap = max(tick_size, best_ask - (tick_size * max(1, buffer_ticks)))
            adjusted_price = min(adjusted_price, maker_cap)
        elif side == "SELL" and best_bid > 0.0:
            maker_floor = min(0.99, best_bid + (tick_size * max(1, buffer_ticks)))
            adjusted_price = max(adjusted_price, maker_floor)

        adjusted_price = max(tick_size, min(0.99, self._round_price_to_tick(adjusted_price, tick_size, side)))
        if side == "BUY" and best_ask > 0.0 and adjusted_price >= best_ask:
            adjusted_price = max(tick_size, best_ask - tick_size)
            adjusted_price = self._round_price_to_tick(adjusted_price, tick_size, side)
        if side == "SELL" and best_bid > 0.0 and adjusted_price <= best_bid:
            adjusted_price = min(0.99, best_bid + tick_size)
            adjusted_price = self._round_price_to_tick(adjusted_price, tick_size, side)

        if adjusted_price <= 0.0:
            return (0.0, 0.0, {}, "live preflight failed: adjusted price invalid")

        size = notional_usd / adjusted_price
        if min_order_size > 0.0 and size < min_order_size:
            return (
                0.0,
                0.0,
                {
                    "tick_size": tick_size,
                    "min_order_size": min_order_size,
                    "best_bid": best_bid,
                    "best_ask": best_ask,
                    "midpoint": midpoint_value,
                    "neg_risk": bool(book.neg_risk),
                },
                f"live preflight rejected: size {size:.6f} below minimum {min_order_size:.6f}",
            )

        snapshot = {
            "tick_size": tick_size,
            "min_order_size": min_order_size,
            "best_bid": best_bid,
            "best_ask": best_ask,
            "midpoint": midpoint_value,
            "neg_risk": bool(book.neg_risk),
            "last_trade_price": self._safe_float(book.last_trade_price),
        }
        return (adjusted_price, size, snapshot, None)

    def _create_and_post_order(
        self,
        order_args: object,
        *,
        tick_size: float,
        neg_risk: bool,
    ) -> object:
        order_payload = {
            "token_id": str(getattr(order_args, "token_id", "") or ""),
            "price": float(self._safe_float(getattr(order_args, "price", 0.0), 0.0)),
            "size": float(self._safe_float(getattr(order_args, "size", 0.0), 0.0)),
            "side": str(getattr(order_args, "side", "") or ""),
            "fee_rate_bps": int(self._safe_float(getattr(order_args, "fee_rate_bps", 0), 0.0)),
            "tick_size": float(self._safe_float(tick_size, 0.01)),
            "neg_risk": bool(neg_risk),
            "chain_id": int(self._chain_id),
            "funder_address": str(self._funder),
        }
        try:
            signed = self._signer_client.sign_order(order_payload)
        except SignerClientError:
            raise
        except Exception as exc:
            raise SignerClientError(
                str(exc),
                reason_code="signer_sign_order_failed",
            ) from exc
        return self.client.post_order(signed, self._OrderType.GTC)

    @staticmethod
    def _parse_timestamp(value: object) -> int:
        if value in (None, ""):
            return 0
        try:
            return int(float(value))
        except (TypeError, ValueError):
            text = str(value).strip()
            if not text:
                return 0
            if text.endswith("Z"):
                text = f"{text[:-1]}+00:00"
            try:
                return int(datetime.fromisoformat(text).timestamp())
            except ValueError:
                return 0

    @staticmethod
    def _normalize_side(value: object) -> str:
        text = str(value or "").strip().upper()
        if text in {"BUY", "BID", "B"} or "BUY" in text or "BID" in text:
            return "BUY"
        if text in {"SELL", "ASK", "S"} or "SELL" in text or "ASK" in text:
            return "SELL"
        return text

    @staticmethod
    def _extract_api_creds(creds: object) -> dict[str, str]:
        if isinstance(creds, dict):
            return {
                "apiKey": str(creds.get("api_key") or creds.get("apiKey") or "").strip(),
                "secret": str(creds.get("secret") or creds.get("api_secret") or "").strip(),
                "passphrase": str(creds.get("passphrase") or creds.get("api_passphrase") or "").strip(),
            }
        return {
            "apiKey": str(getattr(creds, "api_key", getattr(creds, "apiKey", "")) or "").strip(),
            "secret": str(getattr(creds, "secret", getattr(creds, "api_secret", "")) or "").strip(),
            "passphrase": str(getattr(creds, "passphrase", getattr(creds, "api_passphrase", "")) or "").strip(),
        }

    def _parse_order_status(self, order_id: str, payload: object) -> OrderStatusSnapshot | None:
        if not isinstance(payload, dict):
            return None

        status = str(payload.get("status") or payload.get("state") or "").strip().lower()
        original_size = self._safe_float(
            payload.get("originalSize")
            or payload.get("original_size")
            or payload.get("size")
            or payload.get("initialSize")
            or payload.get("amount")
        )
        matched_notional = self._safe_float(
            payload.get("filledNotional")
            or payload.get("filled_notional")
            or payload.get("matchedAmount")
            or payload.get("matched_amount")
        )
        matched_size = self._safe_float(
            payload.get("sizeMatched")
            or payload.get("matchedSize")
            or payload.get("size_matched")
            or payload.get("matched_amount")
            or payload.get("matchedAmount")
            or payload.get("filledSize")
            or payload.get("filled_size")
        )
        avg_fill_price = self._safe_float(
            payload.get("avgPrice")
            or payload.get("matchedPrice")
            or payload.get("price")
        )
        remaining_size = self._safe_float(
            payload.get("remainingSize")
            or payload.get("remaining_size")
            or payload.get("sizeRemaining")
            or payload.get("unfilledSize")
        )
        if remaining_size <= 0.0 and original_size > 0.0 and matched_size > 0.0:
            remaining_size = max(0.0, original_size - matched_size)
        if matched_notional <= 0.0 and matched_size > 0.0 and avg_fill_price > 0.0:
            matched_notional = matched_size * avg_fill_price

        return OrderStatusSnapshot(
            order_id=str(payload.get("orderID") or payload.get("id") or order_id or "").strip(),
            status=status,
            matched_notional=matched_notional,
            matched_size=matched_size,
            avg_fill_price=avg_fill_price,
            original_size=original_size,
            remaining_size=remaining_size,
            message=str(payload.get("error") or payload.get("message") or "").strip(),
        )

    def _iter_order_rows(self, payload: object) -> list[dict[str, Any]]:
        if isinstance(payload, list):
            return [row for row in payload if isinstance(row, dict)]
        if isinstance(payload, dict):
            if isinstance(payload.get("data"), list):
                return [row for row in payload.get("data", []) if isinstance(row, dict)]
            return [payload]
        return []

    def _iter_trade_rows(self, payload: object) -> list[dict[str, Any]]:
        return self._iter_order_rows(payload)

    def get_order_status(self, order_id: str) -> OrderStatusSnapshot | None:
        normalized = str(order_id or "").strip()
        if not normalized:
            return None

        get_order = getattr(self.client, "get_order", None)
        if callable(get_order):
            try:
                snapshot = self._parse_order_status(normalized, get_order(normalized))
            except Exception:
                snapshot = None
            if snapshot is not None:
                return snapshot

        get_orders = getattr(self.client, "get_orders", None)
        if callable(get_orders):
            for args in (({"id": normalized},), ((normalized,),), tuple()):
                try:
                    payload = get_orders(*args)
                except TypeError:
                    continue
                except Exception:
                    return None
                for row in self._iter_order_rows(payload):
                    row_id = str(row.get("orderID") or row.get("id") or "").strip()
                    if row_id == normalized:
                        return self._parse_order_status(normalized, row)
        return None

    def heartbeat(self, order_ids: list[str]) -> bool:
        active_ids = [str(order_id).strip() for order_id in order_ids if str(order_id).strip()]
        if not active_ids:
            return False

        heartbeat = getattr(self.client, "heartbeat", None)
        if callable(heartbeat):
            for args in ((active_ids,), tuple()):
                try:
                    heartbeat(*args)
                    return True
                except TypeError:
                    continue
                except Exception:
                    return False
        return False

    @staticmethod
    def _clean_order_ids(order_ids: list[str]) -> list[str]:
        cleaned: list[str] = []
        for order_id in order_ids:
            normalized = str(order_id or "").strip()
            if normalized and normalized not in cleaned:
                cleaned.append(normalized)
        return cleaned

    def _cancel_method_available(self, method_names: list[str]) -> bool:
        return any(callable(getattr(self.client, method_name, None)) for method_name in method_names)

    def _invoke_cancel_method(
        self,
        method_names: list[str],
        call_variants: list[tuple[tuple[object, ...], dict[str, object]]],
    ) -> object | None:
        for method_name in method_names:
            method = getattr(self.client, method_name, None)
            if not callable(method):
                continue
            for args, kwargs in call_variants:
                try:
                    return method(*args, **kwargs)
                except TypeError:
                    continue
                except Exception as exc:
                    return {
                        "status": "failed",
                        "success": False,
                        "message": str(exc),
                        "error": str(exc),
                        "method": method_name,
                    }
        return None

    @staticmethod
    def _cancel_result(
        order_id: str,
        *,
        status: str,
        ok: bool,
        message: str,
        method: str,
        raw: object,
    ) -> dict[str, object]:
        return {
            "order_id": str(order_id or "").strip(),
            "status": status,
            "ok": bool(ok),
            "message": str(message or "").strip(),
            "method": method,
            "raw": raw,
        }

    def _normalize_cancel_response(
        self,
        response: object,
        *,
        requested_order_ids: list[str],
        default_message: str,
        method: str,
    ) -> list[dict[str, object]] | None:
        order_ids = self._clean_order_ids(requested_order_ids)
        if isinstance(response, list):
            rows: list[dict[str, object]] = []
            for index, item in enumerate(response):
                fallback_order_id = order_ids[index] if index < len(order_ids) else ""
                if isinstance(item, dict):
                    rows.extend(
                        self._normalize_cancel_response(
                            item,
                            requested_order_ids=[fallback_order_id] if fallback_order_id else [],
                            default_message=default_message,
                            method=method,
                        )
                        or []
                    )
                    continue
                if isinstance(item, bool):
                    rows.append(
                        self._cancel_result(
                            fallback_order_id,
                            status="canceled" if item else "failed",
                            ok=bool(item),
                            message=default_message if item else "live cancel rejected",
                            method=method,
                            raw=item,
                        )
                    )
                    continue
                message = str(item or "").strip() or default_message
                rows.append(
                    self._cancel_result(
                        fallback_order_id,
                        status="canceled" if fallback_order_id else "requested",
                        ok=True,
                        message=message,
                        method=method,
                        raw=item,
                    )
                )
            return rows

        if isinstance(response, dict):
            nested_order_ids = self._clean_order_ids(
                [
                    str(item)
                    for key in (
                        "canceledOrderIDs",
                        "canceledOrderIds",
                        "canceled_order_ids",
                        "orderIDs",
                        "orderIds",
                        "order_ids",
                        "ids",
                    )
                    for item in (response.get(key) if isinstance(response.get(key), list) else [])
                ]
            )
            if nested_order_ids:
                return [
                    self._cancel_result(
                        order_id,
                        status="canceled",
                        ok=True,
                        message=default_message,
                        method=method,
                        raw=response,
                    )
                    for order_id in nested_order_ids
                ]

            order_id = str(
                response.get("orderID")
                or response.get("orderId")
                or response.get("order_id")
                or response.get("id")
                or (order_ids[0] if order_ids else "")
            ).strip()
            status = str(response.get("status") or response.get("state") or "").strip().lower()
            if status == "cancelled":
                status = "canceled"
            success = response.get("success")
            if success is False:
                status = "failed"
            elif success is True and not status:
                status = "canceled"
            if not status:
                status = "canceled" if order_id else "requested"
            ok = status not in {"failed", "rejected", "error", "unsupported"}
            message = str(response.get("message") or response.get("error") or default_message or "").strip()
            if not message and ok:
                message = default_message
            return [
                self._cancel_result(
                    order_id,
                    status=status,
                    ok=ok,
                    message=message or default_message,
                    method=method,
                    raw=response,
                )
            ]

        if isinstance(response, bool):
            if not order_ids:
                return None
            status = "canceled" if response else "failed"
            message = default_message if response else "live cancel rejected"
            return [
                self._cancel_result(
                    order_id,
                    status=status,
                    ok=bool(response),
                    message=message,
                    method=method,
                    raw=response,
                )
                for order_id in order_ids
            ]

        if isinstance(response, str):
            message = response.strip() or default_message
            if not order_ids:
                return [
                    self._cancel_result(
                        "",
                        status="requested",
                        ok=True,
                        message=message,
                        method=method,
                        raw=response,
                    )
                ]
            return [
                self._cancel_result(
                    order_id,
                    status="canceled",
                    ok=True,
                    message=message,
                    method=method,
                    raw=response,
                )
                for order_id in order_ids
            ]

        if response is None:
            return None
        if not order_ids:
            return None
        return [
            self._cancel_result(
                order_id,
                status="canceled",
                ok=True,
                message=default_message,
                method=method,
                raw=response,
            )
            for order_id in order_ids
        ]

    def _cancel_order_via_client(self, order_id: str) -> object | None:
        normalized = str(order_id or "").strip()
        if not normalized:
            return None
        return self._invoke_cancel_method(
            ["cancel_order", "delete_order", "cancel"],
            [
                ((normalized,), {}),
                (tuple(), {"order_id": normalized}),
                (tuple(), {"orderID": normalized}),
                (tuple(), {"id": normalized}),
                (({"order_id": normalized},), {}),
                (({"orderID": normalized},), {}),
            ],
        )

    def _cancel_orders_via_client(self, order_ids: list[str]) -> object | None:
        normalized = self._clean_order_ids(order_ids)
        if not normalized:
            return []
        return self._invoke_cancel_method(
            ["cancel_orders", "cancel_order_ids", "delete_orders", "cancel_all_orders"],
            [
                ((normalized,), {}),
                ((tuple(normalized),), {}),
                (tuple(), {"order_ids": normalized}),
                (tuple(), {"orderIDs": normalized}),
                (tuple(), {"ids": normalized}),
            ],
        )

    def cancel_order(self, order_id: str) -> dict[str, object] | None:
        normalized = str(order_id or "").strip()
        if not normalized:
            return None
        rows = self.cancel_orders([normalized])
        if not rows:
            return None
        return rows[0]

    def cancel_orders(self, order_ids: list[str]) -> list[dict[str, object]] | None:
        normalized = self._clean_order_ids(order_ids)
        if not normalized:
            return []

        response = self._cancel_orders_via_client(normalized)
        rows = self._normalize_cancel_response(
            response,
            requested_order_ids=normalized,
            default_message="live cancel requested",
            method="cancel_orders",
        )
        if rows is not None:
            return rows

        batch_available = self._cancel_method_available(["cancel_orders", "cancel_order_ids", "delete_orders", "cancel_all_orders"])
        single_available = self._cancel_method_available(["cancel_order", "delete_order", "cancel"])
        if not batch_available and not single_available:
            return [
                self._cancel_result(
                    order_id,
                    status="unsupported",
                    ok=False,
                    message="live broker cancel_orders method unavailable",
                    method="cancel_orders",
                    raw=None,
                )
                for order_id in normalized
            ]

        rows = []
        for order_id in normalized:
            response = self._cancel_order_via_client(order_id)
            normalized_rows = self._normalize_cancel_response(
                response,
                requested_order_ids=[order_id],
                default_message="live cancel requested",
                method="cancel_order",
            )
            if normalized_rows:
                rows.extend(normalized_rows)
            else:
                rows.append(
                    self._cancel_result(
                        order_id,
                        status="canceled",
                        ok=True,
                        message="live cancel requested",
                        method="cancel_order",
                        raw=response,
                    )
                )
        return rows

    def cancel_open_orders(self) -> list[dict[str, object]] | None:
        open_orders = self.list_open_orders() or []
        order_ids = self._clean_order_ids([order.order_id for order in open_orders if order.order_id])
        if not order_ids:
            if not self._cancel_method_available(["cancel_open_orders", "cancel_all_orders", "cancel_all"]):
                return []
            response = self._invoke_cancel_method(
                ["cancel_open_orders", "cancel_all_orders", "cancel_all"],
                [
                    (tuple(), {}),
                    (tuple(), {"order_ids": []}),
                    (tuple(), {"orderIDs": []}),
                    (tuple(), {"ids": []}),
                ],
            )
            rows = self._normalize_cancel_response(
                response,
                requested_order_ids=[],
                default_message="live cancel-all requested",
                method="cancel_open_orders",
            )
            return rows or []

        rows = self.cancel_orders(order_ids)
        if rows and not all(str(row.get("status") or "").strip().lower() == "unsupported" for row in rows):
            return rows

        if not self._cancel_method_available(["cancel_open_orders", "cancel_all_orders", "cancel_all"]):
            return [
                self._cancel_result(
                    order_id,
                    status="unsupported",
                    ok=False,
                    message="live broker cancel_open_orders method unavailable",
                    method="cancel_open_orders",
                    raw=None,
                )
                for order_id in order_ids
            ]

        response = self._invoke_cancel_method(
            ["cancel_open_orders", "cancel_all_orders", "cancel_all"],
            [
                (tuple(), {}),
                (tuple(), {"order_ids": order_ids}),
                (tuple(), {"orderIDs": order_ids}),
                (tuple(), {"ids": order_ids}),
            ],
        )
        rows = self._normalize_cancel_response(
            response,
            requested_order_ids=order_ids,
            default_message="live cancel-all requested",
            method="cancel_open_orders",
        )
        if rows is not None:
            return rows
        return [
            self._cancel_result(
                order_id,
                status="canceled",
                ok=True,
                message="live cancel-all requested",
                method="cancel_open_orders",
                raw=response,
            )
            for order_id in order_ids
        ]

    def _parse_open_order(self, payload: object) -> OpenOrderSnapshot | None:
        if not isinstance(payload, dict):
            return None
        order_id = str(payload.get("orderID") or payload.get("id") or "").strip()
        token_id = str(
            payload.get("asset_id")
            or payload.get("asset")
            or payload.get("token_id")
            or payload.get("tokenID")
            or ""
        ).strip()
        side = self._normalize_side(payload.get("side"))
        if not order_id or not token_id or side not in {"BUY", "SELL"}:
            return None
        price = self._safe_float(payload.get("price") or payload.get("limitPrice") or payload.get("avgPrice"))
        original_size = self._safe_float(
            payload.get("originalSize")
            or payload.get("original_size")
            or payload.get("size")
            or payload.get("initialSize")
            or payload.get("amount")
        )
        matched_size = self._safe_float(
            payload.get("sizeMatched")
            or payload.get("matchedSize")
            or payload.get("size_matched")
            or payload.get("filledSize")
            or payload.get("filled_size")
            or payload.get("matched_amount")
            or payload.get("matchedAmount")
        )
        remaining_size = self._safe_float(
            payload.get("remainingSize")
            or payload.get("remaining_size")
            or payload.get("sizeRemaining")
            or payload.get("unfilledSize")
        )
        if remaining_size <= 0.0 and original_size > 0.0:
            remaining_size = max(0.0, original_size - matched_size)
        return OpenOrderSnapshot(
            order_id=order_id,
            token_id=token_id,
            side=side,
            status=str(payload.get("status") or payload.get("state") or "").strip(),
            price=price,
            original_size=original_size,
            matched_size=matched_size,
            remaining_size=remaining_size,
            created_ts=self._parse_timestamp(
                payload.get("createdAt")
                or payload.get("created_at")
                or payload.get("created")
                or payload.get("timestamp")
            ),
            condition_id=str(payload.get("conditionId") or payload.get("condition_id") or "").strip(),
            market_slug=str(payload.get("slug") or payload.get("market_slug") or token_id),
            outcome=str(payload.get("outcome") or "YES"),
            message=str(payload.get("error") or payload.get("message") or "").strip(),
        )

    def list_open_orders(self) -> list[OpenOrderSnapshot] | None:
        active_statuses = {"submitted", "posted", "open", "live", "delayed", "partially_filled"}
        payload: object | None = None

        get_open_orders = getattr(self.client, "get_open_orders", None)
        if callable(get_open_orders):
            for args, kwargs in ((tuple(), {}), (({"status": "open"},), {}), (tuple(), {"status": "open"})):
                try:
                    payload = get_open_orders(*args, **kwargs)
                    break
                except TypeError:
                    continue
                except Exception:
                    return None

        if payload is None:
            get_orders = getattr(self.client, "get_orders", None)
            if not callable(get_orders):
                return None
            for args, kwargs in (
                (({"status": "open"},), {}),
                (tuple(), {"status": "open"}),
                (tuple(), {}),
            ):
                try:
                    payload = get_orders(*args, **kwargs)
                    break
                except TypeError:
                    continue
                except Exception:
                    return None

        open_orders: list[OpenOrderSnapshot] = []
        for row in self._iter_order_rows(payload):
            snapshot = self._parse_open_order(row)
            if snapshot is None:
                continue
            if snapshot.lifecycle_status not in active_statuses:
                continue
            open_orders.append(snapshot)
        return open_orders

    def _select_trade_order_id(self, payload: dict[str, Any]) -> str:
        direct = str(
            payload.get("orderID")
            or payload.get("orderId")
            or payload.get("order_id")
            or payload.get("id")
            or ""
        ).strip()
        if direct:
            return direct

        maker_order_id = str(
            payload.get("makerOrderID")
            or payload.get("makerOrderId")
            or payload.get("maker_order_id")
            or ""
        ).strip()
        taker_order_id = str(
            payload.get("takerOrderID")
            or payload.get("takerOrderId")
            or payload.get("taker_order_id")
            or ""
        ).strip()
        maker_wallet = str(
            payload.get("makerAddress")
            or payload.get("makerProxyWallet")
            or payload.get("makerWallet")
            or payload.get("maker_address")
            or payload.get("maker_proxy_wallet")
            or ""
        ).strip().lower()
        taker_wallet = str(
            payload.get("takerAddress")
            or payload.get("takerProxyWallet")
            or payload.get("takerWallet")
            or payload.get("taker_address")
            or payload.get("taker_proxy_wallet")
            or ""
        ).strip().lower()
        owner_side = str(payload.get("ownerSide") or payload.get("role") or "").strip().lower()
        if self._funder:
            if maker_order_id and maker_wallet == self._funder:
                return maker_order_id
            if taker_order_id and taker_wallet == self._funder:
                return taker_order_id
        if owner_side == "maker" and maker_order_id:
            return maker_order_id
        if owner_side == "taker" and taker_order_id:
            return taker_order_id
        return maker_order_id or taker_order_id

    def _parse_recent_fill(self, payload: object) -> OrderFillSnapshot | None:
        if not isinstance(payload, dict):
            return None
        order_id = self._select_trade_order_id(payload)
        token_id = str(
            payload.get("asset")
            or payload.get("asset_id")
            or payload.get("token_id")
            or payload.get("tokenID")
            or ""
        ).strip()
        side = self._normalize_side(payload.get("side") or payload.get("ownerSide"))
        price = self._safe_float(payload.get("price") or payload.get("matchedPrice") or payload.get("avgPrice"))
        size = self._safe_float(
            payload.get("size")
            or payload.get("filledSize")
            or payload.get("filled_size")
            or payload.get("matchedSize")
            or payload.get("size_matched")
            or payload.get("makerAmount")
            or payload.get("takerAmount")
        )
        notional = self._safe_float(
            payload.get("filledNotional")
            or payload.get("filled_notional")
            or payload.get("matchedAmount")
            or payload.get("matched_amount")
            or payload.get("usdcSize")
            or payload.get("usdc_size")
        )
        if size <= 0.0 and notional > 0.0 and price > 0.0:
            size = notional / price
        if price <= 0.0 and size > 0.0 and notional > 0.0:
            price = notional / size
        if not order_id or not token_id or side not in {"BUY", "SELL"} or price <= 0.0 or size <= 0.0:
            return None
        return OrderFillSnapshot(
            order_id=order_id,
            token_id=token_id,
            side=side,
            price=price,
            size=size,
            timestamp=self._parse_timestamp(
                payload.get("timestamp")
                or payload.get("matchedAt")
                or payload.get("createdAt")
                or payload.get("created_at")
            ),
            tx_hash=str(payload.get("transactionHash") or payload.get("txHash") or payload.get("hash") or "").strip(),
            market_slug=str(payload.get("slug") or payload.get("market_slug") or token_id),
            outcome=str(payload.get("outcome") or "YES"),
        )

    def list_recent_fills(
        self,
        *,
        since_ts: int = 0,
        order_ids: list[str] | None = None,
        limit: int = 200,
    ) -> list[OrderFillSnapshot] | None:
        get_trades = getattr(self.client, "get_trades", None)
        if not callable(get_trades):
            return None

        payload: object | None = None
        normalized_ids = {str(order_id).strip() for order_id in (order_ids or []) if str(order_id).strip()}
        for args, kwargs in (
            (tuple(), {}),
            (({"limit": int(limit)},), {}),
            (tuple(), {"limit": int(limit)}),
        ):
            try:
                payload = get_trades(*args, **kwargs)
                break
            except TypeError:
                continue
            except Exception:
                return None
        if payload is None:
            return None

        fills: list[OrderFillSnapshot] = []
        for row in self._iter_trade_rows(payload):
            fill = self._parse_recent_fill(row)
            if fill is None:
                continue
            if since_ts > 0 and fill.timestamp > 0 and fill.timestamp < since_ts:
                continue
            if normalized_ids and fill.order_id not in normalized_ids:
                continue
            fills.append(fill)
        fills.sort(key=lambda item: (int(item.timestamp or 0), item.order_id))
        if limit > 0:
            return fills[-int(limit) :]
        return fills

    def _iter_stream_rows(self, payload: object) -> list[dict[str, Any]]:
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except json.JSONDecodeError:
                return []
        if isinstance(payload, list):
            return [row for row in payload if isinstance(row, dict)]
        if not isinstance(payload, dict):
            return []
        if isinstance(payload.get("events"), list):
            return [row for row in payload.get("events", []) if isinstance(row, dict)]
        if isinstance(payload.get("data"), list):
            return [row for row in payload.get("data", []) if isinstance(row, dict)]
        data = payload.get("data")
        if isinstance(data, dict):
            return [data]
        return [payload]

    @staticmethod
    def _map_stream_order_status(event_type: str, payload: dict[str, Any], *, matched_size: float) -> str:
        status = str(payload.get("status") or payload.get("state") or "").strip().lower()
        if status:
            return status
        normalized = str(event_type or "").strip().lower()
        if normalized in {"placement", "posted"}:
            return "live"
        if normalized in {"cancelation", "cancellation", "cancel"}:
            return "canceled"
        if normalized in {"failed", "rejected"}:
            return "failed"
        if normalized == "update":
            return "partially_filled" if matched_size > 0.0 else "live"
        return "live"

    def _parse_stream_order_event(self, payload: dict[str, Any]) -> BrokerOrderEvent | None:
        order_id = str(payload.get("id") or payload.get("order_id") or payload.get("orderID") or "").strip()
        if not order_id:
            return None
        snapshot = self._parse_order_status(order_id, payload)
        if snapshot is None:
            return None
        matched_size = float(snapshot.matched_size or 0.0)
        status = self._map_stream_order_status(
            str(payload.get("type") or payload.get("eventType") or ""),
            payload,
            matched_size=matched_size,
        )
        if snapshot.normalized_status:
            status = snapshot.normalized_status
        return BrokerOrderEvent(
            event_type="status",
            order_id=str(snapshot.order_id or order_id),
            token_id=str(payload.get("asset_id") or payload.get("token_id") or payload.get("asset") or "").strip(),
            side=self._normalize_side(payload.get("side")),
            timestamp=self._parse_timestamp(
                payload.get("timestamp")
                or payload.get("created_at")
                or payload.get("createdAt")
                or payload.get("last_updated_at")
            ),
            status=str(status or snapshot.lifecycle_status or snapshot.normalized_status or ""),
            matched_notional=float(snapshot.matched_notional or 0.0),
            matched_size=matched_size,
            avg_fill_price=float(snapshot.avg_fill_price or 0.0),
            market_slug=str(payload.get("slug") or payload.get("market_slug") or ""),
            outcome=str(payload.get("outcome") or "YES"),
            message=str(snapshot.message or payload.get("message") or ""),
        )

    def _parse_stream_trade_events(self, payload: dict[str, Any]) -> list[BrokerOrderEvent]:
        fill = self._parse_recent_fill(payload)
        trade_status = str(payload.get("status") or payload.get("state") or "").strip().lower()
        if not fill:
            order_id = self._select_trade_order_id(payload)
            if not order_id:
                return []
            price = self._safe_float(payload.get("price") or payload.get("matchedPrice") or payload.get("avgPrice"))
            size = self._safe_float(
                payload.get("size")
                or payload.get("matched_amount")
                or payload.get("filledSize")
                or payload.get("filled_size")
                or payload.get("matchedSize")
                or payload.get("size_matched")
            )
            notional = self._safe_float(payload.get("usdcSize") or payload.get("matchedAmount") or payload.get("filledNotional"))
            if notional <= 0.0 and size > 0.0 and price > 0.0:
                notional = size * price
            fill = OrderFillSnapshot(
                order_id=order_id,
                token_id=str(payload.get("asset_id") or payload.get("asset") or payload.get("token_id") or "").strip(),
                side=self._normalize_side(payload.get("side") or payload.get("ownerSide")),
                price=price,
                size=size,
                timestamp=self._parse_timestamp(
                    payload.get("timestamp")
                    or payload.get("matchedAt")
                    or payload.get("createdAt")
                    or payload.get("created_at")
                ),
                tx_hash=str(payload.get("transactionHash") or payload.get("txHash") or payload.get("hash") or "").strip(),
                market_slug=str(payload.get("slug") or payload.get("market_slug") or ""),
                outcome=str(payload.get("outcome") or "YES"),
            )
            if not fill.token_id or fill.price <= 0.0 or fill.size <= 0.0 or fill.side not in {"BUY", "SELL"}:
                return []

        events = [
            BrokerOrderEvent(
                event_type="status",
                order_id=fill.order_id,
                token_id=fill.token_id,
                side=fill.side,
                timestamp=int(fill.timestamp or 0),
                status=str(trade_status or "matched"),
                matched_notional=float(fill.notional),
                matched_size=float(fill.size or 0.0),
                avg_fill_price=float(fill.price or 0.0),
                tx_hash=str(fill.tx_hash or ""),
                market_slug=str(fill.market_slug or ""),
                outcome=str(fill.outcome or ""),
            )
        ]
        if trade_status in {"matched", ""}:
            events.insert(
                0,
                BrokerOrderEvent(
                    event_type="fill",
                    order_id=fill.order_id,
                    token_id=fill.token_id,
                    side=fill.side,
                    timestamp=int(fill.timestamp or 0),
                    matched_notional=float(fill.notional),
                    matched_size=float(fill.size or 0.0),
                    avg_fill_price=float(fill.price or 0.0),
                    tx_hash=str(fill.tx_hash or ""),
                    market_slug=str(fill.market_slug or ""),
                    outcome=str(fill.outcome or ""),
                ),
            )
        return events

    def _parse_user_stream_message(self, payload: object) -> list[BrokerOrderEvent]:
        events: list[BrokerOrderEvent] = []
        for row in self._iter_stream_rows(payload):
            event_type = str(row.get("event_type") or row.get("eventType") or row.get("type") or "").strip().lower()
            if event_type == "trade":
                events.extend(self._parse_stream_trade_events(row))
                continue
            if event_type in {"order", "placement", "update", "cancelation", "cancellation", "failed"}:
                event = self._parse_stream_order_event(row)
                if event is not None:
                    events.append(event)
        return self._dedupe_order_events(events)

    @staticmethod
    def _dedupe_order_events(events: list[BrokerOrderEvent]) -> list[BrokerOrderEvent]:
        seen: set[str] = set()
        deduped: list[BrokerOrderEvent] = []
        for event in sorted(events, key=lambda item: (int(item.timestamp or 0), item.order_id, item.normalized_event_type)):
            key = "|".join(
                [
                    str(event.normalized_event_type),
                    str(event.order_id or ""),
                    str(event.token_id or ""),
                    str(event.status or ""),
                    f"{float(event.matched_notional or 0.0):.10f}",
                    f"{float(event.matched_size or 0.0):.10f}",
                    f"{float(event.avg_fill_price or 0.0):.10f}",
                    str(event.tx_hash or ""),
                ]
            )
            if key in seen:
                continue
            seen.add(key)
            deduped.append(event)
        return deduped

    def list_order_events(
        self,
        *,
        since_ts: int = 0,
        order_ids: list[str] | None = None,
        limit: int = 200,
    ) -> list[BrokerOrderEvent] | None:
        normalized_ids = [str(order_id).strip() for order_id in (order_ids or []) if str(order_id).strip()]
        events: list[BrokerOrderEvent] = []
        user_stream = getattr(self, "_user_stream", None)
        if user_stream is not None:
            stream_events = user_stream.events_since(
                since_ts=since_ts,
                order_ids=normalized_ids,
                limit=limit,
            )
            if stream_events:
                events.extend(stream_events)

        fills = self.list_recent_fills(since_ts=since_ts, order_ids=normalized_ids, limit=limit)
        if fills is not None:
            for fill in fills:
                events.append(
                    BrokerOrderEvent(
                        event_type="fill",
                        order_id=fill.order_id,
                        token_id=fill.token_id,
                        side=fill.side,
                        timestamp=int(fill.timestamp or 0),
                        matched_notional=float(fill.notional),
                        matched_size=float(fill.size or 0.0),
                        avg_fill_price=float(fill.price or 0.0),
                        tx_hash=str(fill.tx_hash or ""),
                        market_slug=str(fill.market_slug or ""),
                        outcome=str(fill.outcome or ""),
                    )
                )

        status_ts = max(int(datetime.now().timestamp()), int(since_ts or 0))
        for order_id in normalized_ids:
            snapshot = self.get_order_status(order_id)
            if snapshot is None:
                continue
            events.append(
                BrokerOrderEvent(
                    event_type="status",
                    order_id=snapshot.order_id or order_id,
                    token_id="",
                    side="",
                    timestamp=status_ts,
                    status=str(snapshot.lifecycle_status or snapshot.normalized_status or ""),
                    matched_notional=float(snapshot.matched_notional or 0.0),
                    matched_size=float(snapshot.matched_size or 0.0),
                    avg_fill_price=float(snapshot.avg_fill_price or 0.0),
                    message=str(snapshot.message or ""),
                )
            )

        if not events and fills is None and not normalized_ids:
            return None

        events = self._dedupe_order_events(events)
        events.sort(key=lambda item: (int(item.timestamp or 0), item.order_id, item.normalized_event_type))
        if limit > 0:
            return events[-int(limit) :]
        return events

    def _result_from_response(
        self,
        resp: object,
        *,
        notional_usd: float,
        price: float,
        preflight_snapshot: dict[str, float | bool | str] | None = None,
        strategy_order_uuid: str | None = None,
    ) -> ExecutionResult:
        order_id = None
        message = "live order posted"
        ok = True
        status = ""
        filled_notional = 0.0
        filled_price = 0.0

        if isinstance(resp, dict):
            order_id = str(resp.get("orderID") or resp.get("id") or "").strip() or None
            status = str(resp.get("status") or "").strip().lower()
            success = resp.get("success")
            err = str(resp.get("error") or resp.get("message") or "").strip()
            if success is False or status in {"error", "failed", "rejected", "canceled"}:
                ok = False
                message = err or "live order rejected"
            elif err:
                message = err

            filled_notional = self._safe_float(
                resp.get("filledNotional")
                or resp.get("filled_notional")
                or resp.get("matchedAmount")
                or resp.get("matched_amount")
            )
            filled_size = self._safe_float(
                resp.get("sizeMatched")
                or resp.get("matchedSize")
                or resp.get("size_matched")
                or resp.get("filledSize")
                or resp.get("filled_size")
            )
            filled_price = self._safe_float(
                resp.get("avgPrice")
                or resp.get("matchedPrice")
                or resp.get("price")
            )
            if filled_notional <= 0.0 and filled_size > 0.0 and filled_price > 0.0:
                filled_notional = filled_size * filled_price

        if ok and status in {"matched", "filled"} and filled_notional <= 0.0:
            filled_notional = notional_usd
            filled_price = price
        if ok and filled_notional > 0.0 and filled_price <= 0.0:
            filled_price = price
        if preflight_snapshot:
            details = []
            tick_size = self._safe_float(preflight_snapshot.get("tick_size"))
            min_order_size = self._safe_float(preflight_snapshot.get("min_order_size"))
            midpoint = self._safe_float(preflight_snapshot.get("midpoint"))
            if tick_size > 0.0:
                details.append(f"tick={tick_size:g}")
            if min_order_size > 0.0:
                details.append(f"min_size={min_order_size:g}")
            if midpoint > 0.0:
                details.append(f"mid={midpoint:.4f}")
            details.append(f"px={price:.4f}")
            if details:
                message = f"{message} | {' '.join(details)}"

        metadata: dict[str, object] = {}
        if strategy_order_uuid:
            metadata["strategy_order_uuid"] = strategy_order_uuid
        if preflight_snapshot:
            best_bid = self._safe_float(preflight_snapshot.get("best_bid"))
            best_ask = self._safe_float(preflight_snapshot.get("best_ask"))
            midpoint = self._safe_float(preflight_snapshot.get("midpoint"))
            spread_bps = 0.0
            if best_bid > 0.0 and best_ask > 0.0 and midpoint > 0.0 and best_ask >= best_bid:
                spread_bps = ((best_ask - best_bid) / midpoint) * 10000.0
            requested_vs_mid_bps = 0.0
            if midpoint > 0.0 and price > 0.0:
                requested_vs_mid_bps = ((price - midpoint) / midpoint) * 10000.0
            metadata.update(
                {
                    "best_bid": best_bid,
                    "best_ask": best_ask,
                    "midpoint": midpoint,
                    "tick_size": self._safe_float(preflight_snapshot.get("tick_size")),
                    "min_order_size": self._safe_float(preflight_snapshot.get("min_order_size")),
                    "neg_risk": bool(preflight_snapshot.get("neg_risk")),
                    "last_trade_price": self._safe_float(preflight_snapshot.get("last_trade_price")),
                    "market_spread_bps": spread_bps,
                    "requested_vs_mid_bps": requested_vs_mid_bps,
                    "preflight_has_book": bool(best_bid > 0.0 and best_ask > 0.0 and midpoint > 0.0),
                }
            )

        return ExecutionResult(
            ok=ok,
            broker_order_id=order_id,
            message=message,
            filled_notional=filled_notional if ok else 0.0,
            filled_price=filled_price if ok else 0.0,
            status=status,
            requested_notional=notional_usd,
            requested_price=price,
            metadata=metadata,
        )

    def close(self) -> None:
        user_stream = getattr(self, "_user_stream", None)
        if user_stream is not None:
            user_stream.close()

    def execute(self, signal: Signal, notional_usd: float, *, strategy_order_uuid: str | None = None) -> ExecutionResult:
        side = str(signal.side).upper()
        if side not in self._side_map:
            return ExecutionResult(
                ok=False,
                broker_order_id=None,
                message=f"unsupported side: {signal.side}",
                filled_notional=0.0,
                filled_price=0.0,
                status="rejected",
                requested_notional=notional_usd,
                requested_price=max(0.01, min(0.99, signal.price_hint)),
            )

        price, size, snapshot, preflight_error = self._preflight_order(signal, notional_usd)
        if preflight_error:
            metadata = {
                "best_bid": self._safe_float(snapshot.get("best_bid")),
                "best_ask": self._safe_float(snapshot.get("best_ask")),
                "midpoint": self._safe_float(snapshot.get("midpoint")),
                "tick_size": self._safe_float(snapshot.get("tick_size")),
                "min_order_size": self._safe_float(snapshot.get("min_order_size")),
                "neg_risk": bool(snapshot.get("neg_risk")),
                "last_trade_price": self._safe_float(snapshot.get("last_trade_price")),
                "market_spread_bps": (
                    ((self._safe_float(snapshot.get("best_ask")) - self._safe_float(snapshot.get("best_bid"))) / max(0.0001, self._safe_float(snapshot.get("midpoint")))) * 10000.0
                    if self._safe_float(snapshot.get("best_ask")) > 0.0
                    and self._safe_float(snapshot.get("best_bid")) > 0.0
                    and self._safe_float(snapshot.get("midpoint")) > 0.0
                    else 0.0
                ),
                "requested_vs_mid_bps": (
                    ((max(0.01, min(0.99, signal.price_hint)) - self._safe_float(snapshot.get("midpoint"))) / max(0.0001, self._safe_float(snapshot.get("midpoint")))) * 10000.0
                    if self._safe_float(snapshot.get("midpoint")) > 0.0
                    else 0.0
                ),
                "preflight_has_book": bool(
                    self._safe_float(snapshot.get("best_bid")) > 0.0
                    and self._safe_float(snapshot.get("best_ask")) > 0.0
                    and self._safe_float(snapshot.get("midpoint")) > 0.0
                ),
            }
            if strategy_order_uuid:
                metadata["strategy_order_uuid"] = strategy_order_uuid
            return ExecutionResult(
                ok=False,
                broker_order_id=None,
                message=preflight_error,
                filled_notional=0.0,
                filled_price=0.0,
                status="rejected",
                requested_notional=notional_usd,
                requested_price=max(0.01, min(0.99, signal.price_hint)),
                metadata=metadata,
            )

        order_args = self._OrderArgs(
            token_id=signal.token_id,
            price=price,
            size=size,
            side=self._side_map[side],
        )

        try:
            resp = self._create_and_post_order(
                order_args,
                tick_size=self._safe_float(snapshot.get("tick_size"), 0.01),
                neg_risk=bool(snapshot.get("neg_risk")),
            )
        except SignerClientError as exc:
            reason_code = str(getattr(exc, "reason_code", "") or "signer_sign_order_failed")
            return ExecutionResult(
                ok=False,
                broker_order_id=None,
                message=f"live signer unavailable ({reason_code})",
                filled_notional=0.0,
                filled_price=0.0,
                status="error",
                requested_notional=notional_usd,
                requested_price=price,
                metadata={
                    "reason_code": reason_code,
                    "security_fail_close": True,
                    "security_category": "SIGNER_UNAVAILABLE",
                    "best_bid": self._safe_float(snapshot.get("best_bid")),
                    "best_ask": self._safe_float(snapshot.get("best_ask")),
                    "midpoint": self._safe_float(snapshot.get("midpoint")),
                    "tick_size": self._safe_float(snapshot.get("tick_size")),
                    "min_order_size": self._safe_float(snapshot.get("min_order_size")),
                    "neg_risk": bool(snapshot.get("neg_risk")),
                    "last_trade_price": self._safe_float(snapshot.get("last_trade_price")),
                    "market_spread_bps": (
                        ((self._safe_float(snapshot.get("best_ask")) - self._safe_float(snapshot.get("best_bid"))) / max(0.0001, self._safe_float(snapshot.get("midpoint")))) * 10000.0
                        if self._safe_float(snapshot.get("best_ask")) > 0.0
                        and self._safe_float(snapshot.get("best_bid")) > 0.0
                        and self._safe_float(snapshot.get("midpoint")) > 0.0
                        else 0.0
                    ),
                    "requested_vs_mid_bps": (
                        ((price - self._safe_float(snapshot.get("midpoint"))) / max(0.0001, self._safe_float(snapshot.get("midpoint")))) * 10000.0
                        if self._safe_float(snapshot.get("midpoint")) > 0.0
                        else 0.0
                    ),
                    "preflight_has_book": bool(
                        self._safe_float(snapshot.get("best_bid")) > 0.0
                        and self._safe_float(snapshot.get("best_ask")) > 0.0
                        and self._safe_float(snapshot.get("midpoint")) > 0.0
                    ),
                },
            )
        except Exception:
            return ExecutionResult(
                ok=False,
                broker_order_id=None,
                message="live order error",
                filled_notional=0.0,
                filled_price=0.0,
                status="error",
                requested_notional=notional_usd,
                requested_price=price,
                metadata={
                    "reason_code": "live_order_submit_failed",
                    "best_bid": self._safe_float(snapshot.get("best_bid")),
                    "best_ask": self._safe_float(snapshot.get("best_ask")),
                    "midpoint": self._safe_float(snapshot.get("midpoint")),
                    "tick_size": self._safe_float(snapshot.get("tick_size")),
                    "min_order_size": self._safe_float(snapshot.get("min_order_size")),
                    "neg_risk": bool(snapshot.get("neg_risk")),
                    "last_trade_price": self._safe_float(snapshot.get("last_trade_price")),
                    "market_spread_bps": (
                        ((self._safe_float(snapshot.get("best_ask")) - self._safe_float(snapshot.get("best_bid"))) / max(0.0001, self._safe_float(snapshot.get("midpoint")))) * 10000.0
                        if self._safe_float(snapshot.get("best_ask")) > 0.0
                        and self._safe_float(snapshot.get("best_bid")) > 0.0
                        and self._safe_float(snapshot.get("midpoint")) > 0.0
                        else 0.0
                    ),
                    "requested_vs_mid_bps": (
                        ((price - self._safe_float(snapshot.get("midpoint"))) / max(0.0001, self._safe_float(snapshot.get("midpoint")))) * 10000.0
                        if self._safe_float(snapshot.get("midpoint")) > 0.0
                        else 0.0
                    ),
                    "preflight_has_book": bool(
                        self._safe_float(snapshot.get("best_bid")) > 0.0
                        and self._safe_float(snapshot.get("best_ask")) > 0.0
                        and self._safe_float(snapshot.get("midpoint")) > 0.0
                    ),
                },
            )
        return self._result_from_response(
            resp,
            notional_usd=notional_usd,
            price=price,
            preflight_snapshot=snapshot,
            strategy_order_uuid=strategy_order_uuid,
        )
