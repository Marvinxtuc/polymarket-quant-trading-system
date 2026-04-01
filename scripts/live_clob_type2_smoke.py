#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import time
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from typing import Any

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover
    load_dotenv = None

from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OpenOrderParams, OrderArgs, OrderType, PartialCreateOrderOptions, TradeParams
from py_clob_client.order_builder.constants import BUY, SELL


HOST_DEFAULT = "https://clob.polymarket.com"
CHAIN_ID_DEFAULT = 137
SIZE_QUANT = Decimal("0.01")
USD_QUANT = Decimal("0.01")


def _load_env() -> None:
    if load_dotenv is not None:
        load_dotenv()


def _require_env(name: str) -> str:
    value = str(os.getenv(name, "")).strip()
    if not value:
        raise SystemExit(f"missing required env var: {name}")
    return value


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _normalize_address(value: str) -> str:
    return str(value or "").strip().lower()


def _ceil_size(value: float) -> float:
    quantized = Decimal(str(max(0.0, value))).quantize(SIZE_QUANT, rounding=ROUND_CEILING)
    return float(quantized)


def _floor_size(value: float) -> float:
    quantized = Decimal(str(max(0.0, value))).quantize(SIZE_QUANT, rounding=ROUND_FLOOR)
    return float(quantized)


def _ceil_quantized(value: Decimal, quant: Decimal) -> Decimal:
    if value <= 0:
        return Decimal("0")
    return (value / quant).to_integral_value(rounding=ROUND_CEILING) * quant


def _floor_to_tick(price: float, tick_size: float) -> float:
    tick = Decimal(str(max(0.0001, tick_size)))
    quantized = (Decimal(str(max(0.0, price))) / tick).to_integral_value(rounding=ROUND_FLOOR) * tick
    return float(quantized)


def _tick_string(value: float) -> str:
    text = f"{max(0.0001, value):.4f}".rstrip("0").rstrip(".")
    return text or "0.01"


def _book_level(levels: Any, *, side: str) -> tuple[float, float]:
    if not levels:
        return (0.0, 0.0)
    rows: list[tuple[float, float]] = []
    for level in levels:
        price = _as_float(getattr(level, "price", 0.0))
        size = _as_float(getattr(level, "size", 0.0))
        if price > 0 and size > 0:
            rows.append((price, size))
    if not rows:
        return (0.0, 0.0)
    if str(side).lower() == "bid":
        return max(rows, key=lambda item: item[0])
    return min(rows, key=lambda item: item[0])


def _book_snapshot(client: ClobClient, token_id: str) -> dict[str, Any]:
    book = client.get_order_book(token_id)
    midpoint = client.get_midpoint(token_id)
    if book is None:
        raise SystemExit(f"no book returned for token_id={token_id}")
    best_bid, best_bid_size = _book_level(getattr(book, "bids", None), side="bid")
    best_ask, best_ask_size = _book_level(getattr(book, "asks", None), side="ask")
    tick_size = max(0.0001, _as_float(getattr(book, "tick_size", 0.01), 0.01))
    return {
        "book": book,
        "tick_size": tick_size,
        "tick_text": _tick_string(tick_size),
        "min_order_size": max(0.0, _as_float(getattr(book, "min_order_size", 0.0), 0.0)),
        "neg_risk": bool(getattr(book, "neg_risk", False)),
        "best_bid": best_bid,
        "best_bid_size": best_bid_size,
        "best_ask": best_ask,
        "best_ask_size": best_ask_size,
        "midpoint": _as_float(midpoint, 0.0),
    }


def _resting_buy_price(snapshot: dict[str, Any]) -> float:
    tick_size = float(snapshot["tick_size"])
    best_bid = float(snapshot["best_bid"])
    best_ask = float(snapshot["best_ask"])
    midpoint = float(snapshot["midpoint"])
    price = midpoint if midpoint > 0 else best_bid
    if best_ask > 0:
        price = min(price if price > 0 else best_ask, best_ask - tick_size)
    if best_bid > 0:
        price = min(price if price > 0 else best_bid, best_bid)
    price = _floor_to_tick(max(tick_size, price or tick_size), tick_size)
    if best_ask > 0 and price >= best_ask:
        price = _floor_to_tick(max(tick_size, best_ask - tick_size), tick_size)
    return max(tick_size, min(0.99, price))


def _order_options(snapshot: dict[str, Any]) -> PartialCreateOrderOptions:
    return PartialCreateOrderOptions(
        tick_size=str(snapshot["tick_text"]),
        neg_risk=bool(snapshot["neg_risk"]),
    )


def _choose_aggressive_buy_size(price: float, *, min_size: float, target_usd: float, max_usd: float) -> tuple[float, float]:
    price_dec = Decimal(str(max(0.0, price)))
    if price_dec <= 0:
        raise SystemExit("cannot determine an aggressive BUY price from the current book")

    target_notional = Decimal(str(max(0.0, target_usd))).quantize(USD_QUANT, rounding=ROUND_CEILING)
    max_notional = Decimal(str(max(0.0, max_usd))).quantize(USD_QUANT, rounding=ROUND_FLOOR)
    if max_notional <= 0 or target_notional <= 0:
        raise SystemExit("aggressive BUY target/max notional must be positive")
    if target_notional > max_notional:
        raise SystemExit(f"aggressive BUY target {float(target_notional):.2f} exceeds cap {float(max_notional):.2f}")

    min_size_dec = _ceil_quantized(Decimal(str(max(0.0, min_size))), SIZE_QUANT)
    start_size = max(min_size_dec, _ceil_quantized(target_notional / price_dec, SIZE_QUANT))
    end_size = _ceil_quantized(max_notional / price_dec, SIZE_QUANT)
    if start_size > end_size:
        raise SystemExit(
            f"cannot fit aggressive BUY within ${float(max_notional):.2f} cap at price {float(price_dec):.4f} "
            f"with min_order_size {float(min_size_dec):.2f}"
        )

    size = start_size
    while size <= end_size:
        maker_notional = price_dec * size
        if maker_notional == maker_notional.quantize(USD_QUANT):
            return float(size), float(maker_notional)
        size += SIZE_QUANT

    raise SystemExit(
        f"cannot find aggressive BUY size with cent-precision maker amount between ${float(target_notional):.2f} "
        f"and ${float(max_notional):.2f} at price {float(price_dec):.4f}"
    )


def _select_trade_order_id(row: dict[str, Any], funder: str) -> str:
    direct = str(row.get("orderID") or row.get("orderId") or row.get("order_id") or row.get("id") or "").strip()
    if direct:
        return direct
    maker_order_id = str(row.get("makerOrderID") or row.get("makerOrderId") or row.get("maker_order_id") or "").strip()
    taker_order_id = str(row.get("takerOrderID") or row.get("takerOrderId") or row.get("taker_order_id") or "").strip()
    maker_wallet = _normalize_address(
        row.get("makerAddress")
        or row.get("makerProxyWallet")
        or row.get("makerWallet")
        or row.get("maker_address")
        or row.get("maker_proxy_wallet")
        or ""
    )
    taker_wallet = _normalize_address(
        row.get("takerAddress")
        or row.get("takerProxyWallet")
        or row.get("takerWallet")
        or row.get("taker_address")
        or row.get("taker_proxy_wallet")
        or ""
    )
    owner_side = str(row.get("ownerSide") or row.get("role") or "").strip().lower()
    if funder:
        if maker_order_id and maker_wallet == funder:
            return maker_order_id
        if taker_order_id and taker_wallet == funder:
            return taker_order_id
    if owner_side == "maker" and maker_order_id:
        return maker_order_id
    if owner_side == "taker" and taker_order_id:
        return taker_order_id
    return maker_order_id or taker_order_id


def _extract_order_id(payload: dict[str, Any]) -> str:
    return str(payload.get("orderID") or payload.get("id") or payload.get("orderId") or "").strip()


def _matched_size_from_order(payload: dict[str, Any], fallback_price: float = 0.0) -> float:
    size = _as_float(
        payload.get("sizeMatched")
        or payload.get("matchedSize")
        or payload.get("size_matched")
        or payload.get("filledSize")
        or payload.get("filled_size")
        or 0.0
    )
    if size > 0:
        return size
    price = _as_float(payload.get("avgPrice") or payload.get("matchedPrice") or payload.get("price"), fallback_price)
    notional = _as_float(
        payload.get("filledNotional")
        or payload.get("filled_notional")
        or payload.get("matchedAmount")
        or payload.get("matched_amount")
        or 0.0
    )
    if notional > 0 and price > 0:
        return notional / price
    return 0.0


def _trade_size(row: dict[str, Any]) -> float:
    size = _as_float(
        row.get("size")
        or row.get("filledSize")
        or row.get("filled_size")
        or row.get("matchedSize")
        or row.get("size_matched")
        or row.get("makerAmount")
        or row.get("takerAmount")
        or 0.0
    )
    if size > 0:
        return size
    notional = _as_float(row.get("usdcSize") or row.get("usdc_size") or row.get("matchedAmount") or row.get("matched_amount") or 0.0)
    price = _as_float(row.get("price") or row.get("matchedPrice") or row.get("avgPrice"), 0.0)
    if notional > 0 and price > 0:
        return notional / price
    return 0.0


def _matching_trades(client: ClobClient, order_id: str, funder: str, after_ts: int) -> list[dict[str, Any]]:
    params = TradeParams(after=max(0, int(after_ts) - 5))
    rows = client.get_trades(params)
    if not isinstance(rows, list):
        return []
    matches: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        if _select_trade_order_id(row, funder) != order_id:
            continue
        matches.append(row)
    return matches


def _summarize_order(client: ClobClient, order_id: str, funder: str, after_ts: int, fallback_price: float = 0.0) -> dict[str, Any]:
    order_row = client.get_order(order_id)
    trades = _matching_trades(client, order_id, funder, after_ts)
    matched_from_order = _matched_size_from_order(order_row if isinstance(order_row, dict) else {}, fallback_price)
    matched_from_trades = sum(_trade_size(row) for row in trades)
    return {
        "order": order_row,
        "trade_count": len(trades),
        "matched_size": max(matched_from_order, matched_from_trades),
        "trades": trades,
    }


def _post_limit_order(
    client: ClobClient,
    *,
    token_id: str,
    side: str,
    price: float,
    size: float,
    order_type: str,
    options: PartialCreateOrderOptions,
) -> tuple[str, dict[str, Any]]:
    order = OrderArgs(
        token_id=token_id,
        price=price,
        size=size,
        side=side,
    )
    signed = client.create_order(order, options=options)
    posted = client.post_order(signed, order_type)
    if not isinstance(posted, dict):
        raise SystemExit(f"unexpected post_order response: {posted!r}")
    order_id = _extract_order_id(posted)
    if not order_id:
        raise SystemExit(f"post_order did not return an order id: {posted}")
    return order_id, posted


def _build_aggressive_sell(snapshot: dict[str, Any], filled_buy_size: float) -> tuple[float, float]:
    aggressive_sell_price = float(snapshot["best_bid"] or snapshot["midpoint"] or 0.0)
    if aggressive_sell_price <= 0:
        raise SystemExit("cannot determine an aggressive SELL price from the current book")
    sell_size = min(filled_buy_size, _floor_size(snapshot["best_bid_size"])) if snapshot["best_bid_size"] > 0 else filled_buy_size
    sell_size = _floor_size(sell_size)
    if sell_size <= 0:
        raise SystemExit("no sellable size available after aggressive BUY")
    if sell_size < snapshot["min_order_size"]:
        raise SystemExit(
            f"aggressive SELL size {sell_size:.2f} is below current min_order_size {snapshot['min_order_size']:.2f}; manual unwind required"
        )
    return aggressive_sell_price, sell_size


def _is_retryable_sell_error(exc: Exception) -> bool:
    return "not enough balance / allowance" in str(exc or "").lower()


def _post_aggressive_sell_with_retries(
    client: ClobClient,
    *,
    token_id: str,
    filled_buy_size: float,
    sleep_seconds: float,
    attempts: int = 5,
) -> tuple[str, dict[str, Any], float, float]:
    last_exc: Exception | None = None
    for attempt in range(max(1, int(attempts))):
        snapshot = _book_snapshot(client, token_id)
        aggressive_sell_price, sell_size = _build_aggressive_sell(snapshot, filled_buy_size)
        try:
            aggressive_sell_id, aggressive_sell_post = _post_limit_order(
                client,
                token_id=token_id,
                side=SELL,
                price=aggressive_sell_price,
                size=sell_size,
                order_type=OrderType.FAK,
                options=_order_options(snapshot),
            )
            return aggressive_sell_id, aggressive_sell_post, aggressive_sell_price, sell_size
        except Exception as exc:
            if not _is_retryable_sell_error(exc):
                raise
            last_exc = exc
            if attempt >= max(1, int(attempts)) - 1:
                raise
            time.sleep(max(1.0, sleep_seconds))

    if last_exc is not None:
        raise last_exc
    raise SystemExit("aggressive SELL failed without a captured exception")


def _json_dump(label: str, payload: Any) -> None:
    print(f"\n== {label}")
    print(json.dumps(payload, indent=2, sort_keys=True, default=str))


def main() -> int:
    _load_env()

    parser = argparse.ArgumentParser(description="Minimal live py-clob-client smoke test for type2/proxy wallets.")
    parser.add_argument("--token-id", required=True, help="Conditional token id to trade.")
    parser.add_argument("--resting-usd", type=float, default=1.0, help="Approx notional for the resting BUY.")
    parser.add_argument("--aggressive-usd", type=float, default=1.0, help="Approx notional for aggressive BUY/SELL.")
    parser.add_argument("--max-usd", type=float, default=2.0, help="Hard cap for any live smoke leg.")
    parser.add_argument("--sleep-seconds", type=float, default=2.0, help="Sleep between post/cancel/lookups.")
    parser.add_argument("--yes-live", action="store_true", help="Required. This script submits real live orders.")
    args = parser.parse_args()

    if not args.yes_live:
        raise SystemExit("refusing to submit live orders without --yes-live")

    host = str(os.getenv("POLYMARKET_CLOB_HOST") or os.getenv("CLOB_HOST") or HOST_DEFAULT).strip()
    chain_id = int(os.getenv("CHAIN_ID") or CHAIN_ID_DEFAULT)
    signature_type = int(os.getenv("CLOB_SIGNATURE_TYPE") or 2)
    private_key = _require_env("PRIVATE_KEY")
    funder = _normalize_address(_require_env("FUNDER_ADDRESS"))
    token_id = str(args.token_id).strip()

    client = ClobClient(
        host,
        chain_id=chain_id,
        key=private_key,
        signature_type=signature_type,
        funder=funder,
    )
    creds = client.create_or_derive_api_creds()
    client.set_api_creds(creds)

    _json_dump(
        "auth",
        {
            "host": host,
            "chain_id": chain_id,
            "signature_type": signature_type,
            "signer_address": client.get_address(),
            "funder_address": funder,
            "api_creds_ready": bool(getattr(creds, "api_key", "")),
        },
    )

    snapshot = _book_snapshot(client, token_id)
    _json_dump(
        "book",
        {
            "token_id": token_id,
            "tick_size": snapshot["tick_text"],
            "min_order_size": snapshot["min_order_size"],
            "neg_risk": snapshot["neg_risk"],
            "best_bid": snapshot["best_bid"],
            "best_bid_size": snapshot["best_bid_size"],
            "best_ask": snapshot["best_ask"],
            "best_ask_size": snapshot["best_ask_size"],
            "midpoint": snapshot["midpoint"],
        },
    )

    resting_price = _resting_buy_price(snapshot)
    resting_size = _ceil_size(max(snapshot["min_order_size"], float(args.resting_usd) / resting_price))
    resting_options = _order_options(snapshot)
    resting_started = int(time.time())
    resting_id, resting_post = _post_limit_order(
        client,
        token_id=token_id,
        side=BUY,
        price=resting_price,
        size=resting_size,
        order_type=OrderType.GTC,
        options=resting_options,
    )
    _json_dump(
        "resting_buy_post",
        {
            "price": resting_price,
            "size": resting_size,
            "response": resting_post,
        },
    )
    time.sleep(max(0.0, args.sleep_seconds))

    resting_lookup = client.get_order(resting_id)
    resting_lookup_via_list = client.get_orders(OpenOrderParams(id=resting_id))
    _json_dump(
        "resting_buy_lookup",
        {
            "get_order": resting_lookup,
            "get_orders": resting_lookup_via_list,
        },
    )

    cancel_response = client.cancel(resting_id)
    _json_dump("resting_buy_cancel", cancel_response)
    time.sleep(max(0.0, args.sleep_seconds))
    _json_dump("resting_buy_after_cancel", _summarize_order(client, resting_id, funder, resting_started, resting_price))

    snapshot = _book_snapshot(client, token_id)
    aggressive_buy_price = float(snapshot["best_ask"] or snapshot["midpoint"] or 0.0)
    if aggressive_buy_price <= 0:
        raise SystemExit("cannot determine an aggressive BUY price from the current book")
    aggressive_size, aggressive_buy_notional = _choose_aggressive_buy_size(
        aggressive_buy_price,
        min_size=float(snapshot["min_order_size"]),
        target_usd=float(args.aggressive_usd),
        max_usd=float(args.max_usd),
    )
    if snapshot["best_ask_size"] > 0 and aggressive_size > snapshot["best_ask_size"]:
        raise SystemExit(
            f"requested aggressive BUY size {aggressive_size:.2f} exceeds current best-ask size {snapshot['best_ask_size']:.2f}; lower --aggressive-usd"
        )

    aggressive_buy_started = int(time.time())
    aggressive_buy_id, aggressive_buy_post = _post_limit_order(
        client,
        token_id=token_id,
        side=BUY,
        price=aggressive_buy_price,
        size=aggressive_size,
        order_type=OrderType.FAK,
        options=_order_options(snapshot),
    )
    _json_dump(
        "aggressive_buy_post",
        {
            "price": aggressive_buy_price,
            "size": aggressive_size,
            "notional_usd": aggressive_buy_notional,
            "response": aggressive_buy_post,
        },
    )
    time.sleep(max(0.0, args.sleep_seconds))
    aggressive_buy_summary = _summarize_order(
        client,
        aggressive_buy_id,
        funder,
        aggressive_buy_started,
        aggressive_buy_price,
    )
    _json_dump("aggressive_buy_summary", aggressive_buy_summary)

    filled_buy_size = _floor_size(float(aggressive_buy_summary["matched_size"]))
    if filled_buy_size <= 0:
        raise SystemExit("aggressive BUY did not fill; aborting SELL leg")

    aggressive_sell_started = int(time.time())
    aggressive_sell_id, aggressive_sell_post, aggressive_sell_price, sell_size = _post_aggressive_sell_with_retries(
        client,
        token_id=token_id,
        filled_buy_size=filled_buy_size,
        sleep_seconds=float(args.sleep_seconds),
    )
    _json_dump(
        "aggressive_sell_post",
        {
            "price": aggressive_sell_price,
            "size": sell_size,
            "response": aggressive_sell_post,
        },
    )
    time.sleep(max(0.0, args.sleep_seconds))
    aggressive_sell_summary = _summarize_order(
        client,
        aggressive_sell_id,
        funder,
        aggressive_sell_started,
        aggressive_sell_price,
    )
    _json_dump("aggressive_sell_summary", aggressive_sell_summary)

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        raise SystemExit(130)
