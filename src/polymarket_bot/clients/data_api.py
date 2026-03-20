from __future__ import annotations

import csv
from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
import io
import json
from typing import Any
import zipfile

import httpx
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential


def _is_retryable_http_error(exc: BaseException) -> bool:
    if isinstance(exc, httpx.HTTPStatusError):
        response = exc.response
        status_code = int(getattr(response, "status_code", 0) or 0)
        return status_code == 429 or status_code >= 500
    return isinstance(exc, httpx.RequestError)


@dataclass(slots=True)
class Position:
    wallet: str
    token_id: str
    market_slug: str
    outcome: str
    avg_price: float
    size: float
    notional: float
    timestamp: int
    condition_id: str = ""


@dataclass(slots=True)
class TradeFill:
    wallet: str
    side: str
    token_id: str
    condition_id: str
    market_slug: str
    outcome: str
    price: float
    size: float
    timestamp: int
    tx_hash: str


@dataclass(slots=True)
class OrderBookLevel:
    price: float
    size: float


@dataclass(slots=True)
class OrderBookSummary:
    market: str
    asset_id: str
    timestamp: str
    hash: str
    bids: tuple[OrderBookLevel, ...]
    asks: tuple[OrderBookLevel, ...]
    min_order_size: float
    tick_size: float
    neg_risk: bool
    last_trade_price: float = 0.0

    @property
    def best_bid(self) -> float:
        if not self.bids:
            return 0.0
        return float(self.bids[0].price)

    @property
    def best_ask(self) -> float:
        if not self.asks:
            return 0.0
        return float(self.asks[0].price)


@dataclass(slots=True)
class PriceHistoryPoint:
    market: str
    timestamp: int
    price: float
    interval: str = ""
    fidelity: int = 0


@dataclass(slots=True)
class ClosedPosition:
    wallet: str
    token_id: str
    condition_id: str
    market_slug: str
    outcome: str
    avg_price: float
    total_bought: float
    realized_pnl: float
    timestamp: int
    end_date: str


@dataclass(slots=True)
class ActivityEvent:
    wallet: str
    activity_type: str
    token_id: str
    condition_id: str
    market_slug: str
    outcome: str
    side: str
    price: float
    size: float
    usdc_size: float
    timestamp: int
    tx_hash: str


@dataclass(slots=True)
class AccountingPosition:
    token_id: str
    condition_id: str
    size: float
    price: float
    value: float
    valuation_time: str


@dataclass(slots=True)
class AccountingSnapshot:
    wallet: str
    cash_balance: float
    positions_value: float
    equity: float
    valuation_time: str
    positions: tuple[AccountingPosition, ...] = ()


@dataclass(slots=True)
class ResolvedMarket:
    condition_id: str
    winner_token_id: str | None
    winner_outcome: str | None
    closed: bool


class PolymarketDataClient:
    def __init__(
        self,
        base_url: str,
        timeout_s: float = 15.0,
        market_base_url: str = "https://clob.polymarket.com",
        gamma_base_url: str = "https://gamma-api.polymarket.com",
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.market_base_url = market_base_url.rstrip("/")
        self.gamma_base_url = gamma_base_url.rstrip("/")
        self._client = httpx.Client(timeout=timeout_s)

    def close(self) -> None:
        self._client.close()

    @retry(
        retry=retry_if_exception(_is_retryable_http_error),
        wait=wait_exponential(multiplier=0.5, min=0.5, max=4),
        stop=stop_after_attempt(4),
    )
    def _get_json_from_base(
        self,
        base_url: str,
        path: str,
        params: dict[str, Any] | None = None,
    ) -> Any:
        url = f"{base_url}{path}"
        r = self._client.get(url, params=params)
        r.raise_for_status()
        return r.json()

    @retry(
        retry=retry_if_exception(_is_retryable_http_error),
        wait=wait_exponential(multiplier=0.5, min=0.5, max=4),
        stop=stop_after_attempt(4),
    )
    def _get_bytes_from_base(
        self,
        base_url: str,
        path: str,
        params: dict[str, Any] | None = None,
    ) -> bytes:
        url = f"{base_url}{path}"
        r = self._client.get(url, params=params)
        r.raise_for_status()
        return r.content

    def _get_json(self, path: str, params: dict[str, Any] | None = None) -> Any:
        return self._get_json_from_base(self.base_url, path, params=params)

    @staticmethod
    def _normalize_wallet(wallet: str) -> str:
        return str(wallet).strip().lower()

    @staticmethod
    def _coerce_float(value: Any, default: float = 0.0) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _coerce_int(value: Any, default: int = 0) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _csv_param(values: Sequence[str | int] | None) -> str | None:
        if not values:
            return None
        parts = [str(value).strip() for value in values if str(value).strip()]
        if not parts:
            return None
        return ",".join(parts)

    @staticmethod
    def _parse_string_list(value: Any) -> list[str]:
        if isinstance(value, list):
            return [str(item).strip() for item in value if str(item).strip()]
        if not isinstance(value, str) or not value.strip():
            return []
        text = value.strip()
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            return [text]
        if isinstance(parsed, list):
            return [str(item).strip() for item in parsed if str(item).strip()]
        return []

    def get_active_positions(self, wallet: str, limit: int = 200) -> list[Position]:
        data = self._get_json(
            "/positions",
            params={"user": self._normalize_wallet(wallet), "sizeThreshold": 0, "limit": limit},
        )
        positions: list[Position] = []
        if not isinstance(data, list):
            return positions

        now_ts = int(datetime.now(tz=timezone.utc).timestamp())
        for row in data:
            if not isinstance(row, dict):
                continue
            token_id = str(row.get("asset") or "").strip()
            if not token_id:
                continue

            size = self._coerce_float(row.get("size"))
            if size <= 0:
                continue

            avg_price = self._coerce_float(row.get("avgPrice") or row.get("curPrice"))
            if avg_price <= 0:
                avg_price = 0.5

            notional = size * avg_price
            positions.append(
                Position(
                    wallet=self._normalize_wallet(wallet),
                    token_id=token_id,
                    market_slug=str(row.get("slug") or ""),
                    outcome=str(row.get("outcome") or ""),
                    avg_price=avg_price,
                    size=size,
                    notional=notional,
                    timestamp=self._coerce_int(row.get("timestamp"), default=now_ts),
                    condition_id=str(row.get("conditionId") or "").strip(),
                )
            )
        return positions

    def get_order_book(self, token_id: str) -> OrderBookSummary | None:
        normalized = str(token_id).strip()
        if not normalized:
            return None
        data = self._get_json_from_base(
            self.market_base_url,
            "/book",
            params={"token_id": normalized},
        )
        if not isinstance(data, dict):
            return None
        return self._parse_order_book(data)

    def _parse_order_book(self, row: dict[str, Any]) -> OrderBookSummary | None:
        asset_id = str(row.get("asset_id") or row.get("assetId") or "").strip()
        market = str(row.get("market") or "").strip()
        if not asset_id and not market:
            return None

        def parse_levels(value: Any) -> tuple[OrderBookLevel, ...]:
            levels: list[OrderBookLevel] = []
            if not isinstance(value, list):
                return tuple(levels)
            for raw_level in value:
                if not isinstance(raw_level, dict):
                    continue
                price = self._coerce_float(raw_level.get("price"))
                size = self._coerce_float(raw_level.get("size"))
                if price <= 0.0 or size <= 0.0:
                    continue
                levels.append(OrderBookLevel(price=price, size=size))
            return tuple(levels)

        return OrderBookSummary(
            market=market,
            asset_id=asset_id,
            timestamp=str(row.get("timestamp") or ""),
            hash=str(row.get("hash") or ""),
            bids=parse_levels(row.get("bids")),
            asks=parse_levels(row.get("asks")),
            min_order_size=self._coerce_float(row.get("min_order_size") or row.get("minOrderSize")),
            tick_size=self._coerce_float(row.get("tick_size") or row.get("tickSize")),
            neg_risk=bool(row.get("neg_risk") or row.get("negRisk")),
            last_trade_price=self._coerce_float(row.get("last_trade_price") or row.get("lastTradePrice")),
        )

    def get_midpoint_price(self, token_id: str) -> float | None:
        normalized = str(token_id).strip()
        if not normalized:
            return None
        data = self._get_json_from_base(
            self.market_base_url,
            "/midpoint",
            params={"token_id": normalized},
        )
        if not isinstance(data, dict):
            return None
        midpoint = self._coerce_float(data.get("mid_price") or data.get("mid"))
        if midpoint <= 0.0:
            return None
        return midpoint

    def get_prices_history(
        self,
        token_id: str,
        *,
        start_ts: int | None = None,
        end_ts: int | None = None,
        interval: str | None = None,
        fidelity: int | None = 1,
    ) -> list[PriceHistoryPoint]:
        normalized = str(token_id).strip()
        if not normalized:
            return []

        params: dict[str, Any] = {"market": normalized}
        if start_ts is not None:
            params["startTs"] = int(start_ts)
        if end_ts is not None:
            params["endTs"] = int(end_ts)
        if interval:
            params["interval"] = str(interval).strip()
        if fidelity is not None:
            params["fidelity"] = max(1, int(fidelity))

        data = self._get_json_from_base(self.market_base_url, "/prices-history", params=params)
        rows: list[dict[str, Any]] = []
        if isinstance(data, dict):
            for key in ("history", "data", "prices"):
                value = data.get(key)
                if isinstance(value, list):
                    rows = [row for row in value if isinstance(row, dict)]
                    break
        elif isinstance(data, list):
            rows = [row for row in data if isinstance(row, dict)]

        points: list[PriceHistoryPoint] = []
        for row in rows:
            timestamp = self._coerce_int(row.get("t") or row.get("timestamp") or row.get("ts"))
            price = self._coerce_float(row.get("p") or row.get("price"))
            if timestamp <= 0 or price <= 0.0:
                continue
            points.append(
                PriceHistoryPoint(
                    market=normalized,
                    timestamp=timestamp,
                    price=price,
                    interval=str(interval or ""),
                    fidelity=max(1, int(fidelity or 1)),
                )
            )

        points.sort(key=lambda point: (point.timestamp, point.price))
        deduped: dict[int, PriceHistoryPoint] = {}
        for point in points:
            deduped[point.timestamp] = point
        return list(deduped.values())

    def get_price_history(
        self,
        token_id: str,
        *,
        start_ts: int | None = None,
        end_ts: int | None = None,
        interval: str | None = None,
        fidelity: int | None = 1,
    ) -> list[PriceHistoryPoint]:
        return self.get_prices_history(
            token_id,
            start_ts=start_ts,
            end_ts=end_ts,
            interval=interval,
            fidelity=fidelity,
        )

    def get_user_trades(
        self,
        wallet: str,
        *,
        limit: int = 1000,
        offset: int = 0,
        taker_only: bool = False,
        markets: Sequence[str] | None = None,
        event_ids: Sequence[int] | None = None,
        side: str | None = None,
    ) -> list[TradeFill]:
        params: dict[str, Any] = {
            "user": self._normalize_wallet(wallet),
            "limit": max(0, int(limit)),
            "offset": max(0, int(offset)),
            "takerOnly": bool(taker_only),
        }
        market_param = self._csv_param(markets)
        if market_param:
            params["market"] = market_param
        event_param = self._csv_param(event_ids)
        if event_param:
            params["eventId"] = event_param
        if side:
            params["side"] = str(side).strip().upper()

        data = self._get_json("/trades", params=params)
        fills: list[TradeFill] = []
        if not isinstance(data, list):
            return fills

        for row in data:
            if not isinstance(row, dict):
                continue
            token_id = str(row.get("asset") or "").strip()
            condition_id = str(row.get("conditionId") or "").strip()
            if not token_id or not condition_id:
                continue
            fills.append(
                TradeFill(
                    wallet=str(row.get("proxyWallet") or params["user"]).strip().lower(),
                    side=str(row.get("side") or "").strip().upper(),
                    token_id=token_id,
                    condition_id=condition_id,
                    market_slug=str(row.get("slug") or ""),
                    outcome=str(row.get("outcome") or ""),
                    price=self._coerce_float(row.get("price")),
                    size=self._coerce_float(row.get("size")),
                    timestamp=self._coerce_int(row.get("timestamp")),
                    tx_hash=str(row.get("transactionHash") or ""),
                )
            )
        return fills

    def iter_user_trades(
        self,
        wallet: str,
        *,
        page_size: int = 1000,
        max_pages: int = 20,
        taker_only: bool = False,
        markets: Sequence[str] | None = None,
        event_ids: Sequence[int] | None = None,
        side: str | None = None,
    ) -> Iterator[TradeFill]:
        size = max(1, int(page_size))
        for page_index in range(max(1, int(max_pages))):
            offset = page_index * size
            page = self.get_user_trades(
                wallet,
                limit=size,
                offset=offset,
                taker_only=taker_only,
                markets=markets,
                event_ids=event_ids,
                side=side,
            )
            if not page:
                return
            yield from page
            if len(page) < size:
                return

    def get_closed_positions(
        self,
        wallet: str,
        *,
        limit: int = 50,
        offset: int = 0,
        markets: Sequence[str] | None = None,
        event_ids: Sequence[int] | None = None,
        title: str | None = None,
        sort_by: str = "REALIZEDPNL",
        sort_direction: str = "DESC",
    ) -> list[ClosedPosition]:
        params: dict[str, Any] = {
            "user": self._normalize_wallet(wallet),
            "limit": min(50, max(0, int(limit))),
            "offset": max(0, int(offset)),
            "sortBy": sort_by,
            "sortDirection": sort_direction,
        }
        market_param = self._csv_param(markets)
        if market_param:
            params["market"] = market_param
        event_param = self._csv_param(event_ids)
        if event_param:
            params["eventId"] = event_param
        if title:
            params["title"] = str(title).strip()

        data = self._get_json("/closed-positions", params=params)
        positions: list[ClosedPosition] = []
        if not isinstance(data, list):
            return positions

        for row in data:
            if not isinstance(row, dict):
                continue
            token_id = str(row.get("asset") or "").strip()
            condition_id = str(row.get("conditionId") or "").strip()
            if not token_id or not condition_id:
                continue
            positions.append(
                ClosedPosition(
                    wallet=str(row.get("proxyWallet") or params["user"]).strip().lower(),
                    token_id=token_id,
                    condition_id=condition_id,
                    market_slug=str(row.get("slug") or ""),
                    outcome=str(row.get("outcome") or ""),
                    avg_price=self._coerce_float(row.get("avgPrice")),
                    total_bought=self._coerce_float(row.get("totalBought")),
                    realized_pnl=self._coerce_float(row.get("realizedPnl")),
                    timestamp=self._coerce_int(row.get("timestamp")),
                    end_date=str(row.get("endDate") or ""),
                )
            )
        return positions

    def iter_closed_positions(
        self,
        wallet: str,
        *,
        page_size: int = 50,
        max_pages: int = 20,
        markets: Sequence[str] | None = None,
        event_ids: Sequence[int] | None = None,
        title: str | None = None,
        sort_by: str = "REALIZEDPNL",
        sort_direction: str = "DESC",
    ) -> Iterator[ClosedPosition]:
        size = min(50, max(1, int(page_size)))
        for page_index in range(max(1, int(max_pages))):
            offset = page_index * size
            page = self.get_closed_positions(
                wallet,
                limit=size,
                offset=offset,
                markets=markets,
                event_ids=event_ids,
                title=title,
                sort_by=sort_by,
                sort_direction=sort_direction,
            )
            if not page:
                return
            yield from page
            if len(page) < size:
                return

    def get_accounting_snapshot(self, wallet: str) -> AccountingSnapshot | None:
        normalized = self._normalize_wallet(wallet)
        if not normalized:
            return None

        payload = self._get_bytes_from_base(
            self.base_url,
            "/v1/accounting/snapshot",
            params={"user": normalized},
        )
        if not payload:
            return None

        try:
            archive = zipfile.ZipFile(io.BytesIO(payload))
        except zipfile.BadZipFile:
            return None

        with archive:
            names = {name.rsplit("/", 1)[-1]: name for name in archive.namelist()}
            positions: list[AccountingPosition] = []
            valuation_time = ""
            if "positions.csv" in names:
                with archive.open(names["positions.csv"], "r") as raw:
                    reader = csv.DictReader(io.TextIOWrapper(raw, encoding="utf-8"))
                    for row in reader:
                        token_id = str(row.get("asset") or row.get("token_id") or "").strip()
                        condition_id = str(row.get("conditionId") or row.get("condition_id") or "").strip()
                        size = self._coerce_float(row.get("size"))
                        price = self._coerce_float(row.get("curPrice") or row.get("price"))
                        value = self._coerce_float(row.get("currentValue") or row.get("value"))
                        if value <= 0.0 and size > 0.0 and price > 0.0:
                            value = size * price
                        if not token_id or size <= 0.0 or value <= 0.0:
                            continue
                        row_valuation_time = str(row.get("valuationTime") or row.get("valuation_time") or "").strip()
                        if row_valuation_time and not valuation_time:
                            valuation_time = row_valuation_time
                        positions.append(
                            AccountingPosition(
                                token_id=token_id,
                                condition_id=condition_id,
                                size=size,
                                price=price,
                                value=value,
                                valuation_time=row_valuation_time,
                            )
                        )

            cash_balance = 0.0
            positions_value = 0.0
            equity = 0.0
            equity_row_seen = False
            if "equity.csv" in names:
                with archive.open(names["equity.csv"], "r") as raw:
                    reader = csv.DictReader(io.TextIOWrapper(raw, encoding="utf-8"))
                    row = next(reader, None)
                    if row:
                        equity_row_seen = True
                        cash_balance = self._coerce_float(row.get("cashBalance") or row.get("cash_balance"))
                        positions_value = self._coerce_float(row.get("positionsValue") or row.get("positions_value"))
                        equity = self._coerce_float(row.get("totalValue") or row.get("equity"))
                        valuation_time = (
                            str(row.get("valuationTime") or row.get("valuation_time") or "").strip()
                            or valuation_time
                        )

            if positions_value <= 0.0 and positions:
                positions_value = sum(position.value for position in positions)
            if equity <= 0.0 and (cash_balance > 0.0 or positions_value > 0.0):
                equity = cash_balance + positions_value
            if not valuation_time and positions:
                valuation_time = str(positions[0].valuation_time or "")

            if (not equity_row_seen) and not positions:
                return None

            return AccountingSnapshot(
                wallet=normalized,
                cash_balance=cash_balance,
                positions_value=positions_value,
                equity=equity,
                valuation_time=valuation_time,
                positions=tuple(positions),
            )

    def get_user_activity(
        self,
        wallet: str,
        *,
        limit: int = 500,
        offset: int = 0,
        types: Sequence[str] | None = None,
        start_ts: int | None = None,
        end_ts: int | None = None,
        markets: Sequence[str] | None = None,
        event_ids: Sequence[int] | None = None,
        side: str | None = None,
        sort_by: str = "TIMESTAMP",
        sort_direction: str = "DESC",
    ) -> list[ActivityEvent]:
        params: dict[str, Any] = {
            "user": self._normalize_wallet(wallet),
            "limit": min(500, max(0, int(limit))),
            "offset": max(0, int(offset)),
            "sortBy": sort_by,
            "sortDirection": sort_direction,
        }
        type_param = self._csv_param([str(value).strip().upper() for value in types or []])
        if type_param:
            params["type"] = type_param
        market_param = self._csv_param(markets)
        if market_param:
            params["market"] = market_param
        event_param = self._csv_param(event_ids)
        if event_param:
            params["eventId"] = event_param
        if start_ts is not None:
            params["start"] = int(start_ts)
        if end_ts is not None:
            params["end"] = int(end_ts)
        if side:
            params["side"] = str(side).strip().upper()

        data = self._get_json("/activity", params=params)
        events: list[ActivityEvent] = []
        if not isinstance(data, list):
            return events

        for row in data:
            if not isinstance(row, dict):
                continue
            events.append(
                ActivityEvent(
                    wallet=str(row.get("proxyWallet") or params["user"]).strip().lower(),
                    activity_type=str(row.get("type") or "").strip().upper(),
                    token_id=str(row.get("asset") or "").strip(),
                    condition_id=str(row.get("conditionId") or "").strip(),
                    market_slug=str(row.get("slug") or ""),
                    outcome=str(row.get("outcome") or ""),
                    side=str(row.get("side") or "").strip().upper(),
                    price=self._coerce_float(row.get("price")),
                    size=self._coerce_float(row.get("size")),
                    usdc_size=self._coerce_float(row.get("usdcSize")),
                    timestamp=self._coerce_int(row.get("timestamp")),
                    tx_hash=str(row.get("transactionHash") or ""),
                )
            )
        return events

    def iter_user_activity(
        self,
        wallet: str,
        *,
        page_size: int = 500,
        max_pages: int = 20,
        types: Sequence[str] | None = None,
        start_ts: int | None = None,
        end_ts: int | None = None,
        markets: Sequence[str] | None = None,
        event_ids: Sequence[int] | None = None,
        side: str | None = None,
        sort_by: str = "TIMESTAMP",
        sort_direction: str = "DESC",
    ) -> Iterator[ActivityEvent]:
        size = min(500, max(1, int(page_size)))
        for page_index in range(max(1, int(max_pages))):
            offset = page_index * size
            page = self.get_user_activity(
                wallet,
                limit=size,
                offset=offset,
                types=types,
                start_ts=start_ts,
                end_ts=end_ts,
                markets=markets,
                event_ids=event_ids,
                side=side,
                sort_by=sort_by,
                sort_direction=sort_direction,
            )
            if not page:
                return
            yield from page
            if len(page) < size:
                return

    def get_simplified_markets(self, *, next_cursor: str | None = None) -> dict[str, Any]:
        params: dict[str, Any] = {}
        if next_cursor:
            params["next_cursor"] = next_cursor
        data = self._get_json_from_base(self.market_base_url, "/simplified-markets", params=params or None)
        if isinstance(data, dict):
            return data
        return {"limit": 0, "count": 0, "next_cursor": "LTE=", "data": []}

    def get_market_resolution(
        self,
        condition_id: str,
        *,
        slug: str | None = None,
    ) -> ResolvedMarket | None:
        normalized = str(condition_id).strip()
        if not normalized:
            return None

        candidates: list[dict[str, Any]] = []
        if slug:
            by_slug = self._get_json_from_base(
                self.gamma_base_url,
                "/markets",
                params={"slug": str(slug).strip()},
            )
            if isinstance(by_slug, list):
                candidates.extend(row for row in by_slug if isinstance(row, dict))

        if not candidates:
            direct = self._get_json_from_base(
                self.gamma_base_url,
                "/markets",
                params={"conditionId": normalized},
            )
            if isinstance(direct, list):
                candidates.extend(row for row in direct if isinstance(row, dict))

        for row in candidates:
            parsed = self._parse_gamma_market_resolution(row)
            if parsed is None:
                continue
            if parsed.condition_id == normalized:
                return parsed
        return None

    def _parse_gamma_market_resolution(self, row: dict[str, Any]) -> ResolvedMarket | None:
        condition_id = str(row.get("conditionId") or row.get("condition_id") or "").strip()
        if not condition_id:
            return None

        token_ids = self._parse_string_list(row.get("clobTokenIds") or row.get("clobTokenIds"))
        outcomes = self._parse_string_list(row.get("outcomes"))
        outcome_prices = [
            self._coerce_float(value, default=-1.0)
            for value in self._parse_string_list(row.get("outcomePrices") or row.get("outcome_prices"))
        ]

        winner_token_id: str | None = None
        winner_outcome: str | None = None
        if token_ids and outcome_prices and len(token_ids) == len(outcome_prices):
            max_price = max(outcome_prices)
            winner_indices = [
                index
                for index, price in enumerate(outcome_prices)
                if abs(price - max_price) < 1e-9
            ]
            if max_price >= 0.99 and len(winner_indices) == 1:
                winner_index = winner_indices[0]
                winner_token_id = token_ids[winner_index]
                if winner_index < len(outcomes):
                    winner_outcome = outcomes[winner_index] or None

        return ResolvedMarket(
            condition_id=condition_id,
            winner_token_id=winner_token_id,
            winner_outcome=winner_outcome,
            closed=bool(row.get("closed")),
        )

    def build_resolution_map(
        self,
        condition_ids: set[str],
        *,
        market_slugs: dict[str, str] | None = None,
        max_pages: int = 200,
    ) -> dict[str, ResolvedMarket]:
        wanted = {str(condition_id).strip() for condition_id in condition_ids if str(condition_id).strip()}
        if not wanted:
            return {}

        mapping: dict[str, ResolvedMarket] = {}
        unresolved: set[str] = set()
        for condition_id in wanted:
            try:
                resolved = self.get_market_resolution(
                    condition_id,
                    slug=(market_slugs or {}).get(condition_id),
                )
            except Exception:
                unresolved.add(condition_id)
                continue
            if resolved is None:
                unresolved.add(condition_id)
                continue
            mapping[condition_id] = resolved

        if unresolved:
            wanted = unresolved
        else:
            return mapping

        next_cursor: str | None = None
        for _ in range(max(1, int(max_pages))):
            page = self.get_simplified_markets(next_cursor=next_cursor)
            rows = page.get("data")
            if not isinstance(rows, list) or not rows:
                break

            for row in rows:
                if not isinstance(row, dict):
                    continue
                condition_id = str(row.get("condition_id") or "").strip()
                if condition_id not in wanted:
                    continue

                winner_token_id: str | None = None
                winner_outcome: str | None = None
                tokens = row.get("tokens")
                if isinstance(tokens, list):
                    for token in tokens:
                        if not isinstance(token, dict) or not token.get("winner"):
                            continue
                        winner_token_id = str(token.get("token_id") or "").strip() or None
                        winner_outcome = str(token.get("outcome") or "").strip() or None
                        break

                mapping[condition_id] = ResolvedMarket(
                    condition_id=condition_id,
                    winner_token_id=winner_token_id,
                    winner_outcome=winner_outcome,
                    closed=bool(row.get("closed")),
                )

            if wanted.issubset(mapping):
                break

            next_cursor_value = str(page.get("next_cursor") or "").strip()
            if not next_cursor_value or next_cursor_value == "LTE=":
                break
            next_cursor = next_cursor_value
        return mapping

    @staticmethod
    def _extract_wallet_candidates(row: dict[str, Any]) -> list[str]:
        candidates: set[str] = set()
        keys = (
            "user",
            "owner",
            "wallet",
            "walletAddress",
            "proxyWallet",
            "maker",
            "taker",
        )
        for key in keys:
            value = row.get(key)
            if isinstance(value, str):
                text = value.strip().lower()
                if text.startswith("0x") and len(text) == 42:
                    candidates.add(text)
        return list(candidates)

    def discover_wallet_activity(
        self,
        paths: list[str],
        limit: int = 300,
    ) -> dict[str, int]:
        counts: dict[str, int] = {}
        for path in paths:
            try:
                data = self._get_json(path, params={"limit": limit})
            except Exception:
                # Discovery should be best-effort per endpoint; skip incompatible or failing paths.
                continue
            if not isinstance(data, list):
                continue
            for row in data:
                if not isinstance(row, dict):
                    continue
                for wallet in self._extract_wallet_candidates(row):
                    counts[wallet] = counts.get(wallet, 0) + 1
        return counts
