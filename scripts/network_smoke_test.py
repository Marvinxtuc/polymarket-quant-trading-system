#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
import urllib.parse
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from polymarket_bot.clients.data_api import PolymarketDataClient, Position
from polymarket_bot.wallet_scoring import SmartWalletScorer, build_realized_wallet_metrics


COOKIE_RESTRICTED_RE = re.compile(r'"restricted"\s*:\s*"(true|false)"', re.IGNORECASE)
WALLET_RE = re.compile(r"^0x[a-f0-9]{40}$")


def load_env(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}

    data: dict[str, str] = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        data[key.strip()] = value.strip()
    return data


def parse_restricted_cookie(headers: httpx.Headers) -> tuple[bool, str | None]:
    set_cookie_items = headers.get_list("set-cookie")
    for item in set_cookie_items:
        if "pm_geo=" not in item:
            continue
        text = urllib.parse.unquote(item)
        m = COOKIE_RESTRICTED_RE.search(text)
        if m:
            return (m.group(1).lower() == "true", m.group(1).lower())
    return False, None


def parse_wallets(raw: str) -> list[str]:
    seen: set[str] = set()
    wallets: list[str] = []
    for item in raw.split(","):
        wallet = item.strip().lower()
        if not WALLET_RE.match(wallet) or wallet in seen:
            continue
        wallets.append(wallet)
        seen.add(wallet)
    return wallets


def first_non_empty(*values: str | None) -> str:
    for value in values:
        if value and value.strip():
            return value
    return ""


def summarize_positions(positions: list[Position]) -> tuple[float, int, float]:
    total_notional = sum(max(0.0, p.notional) for p in positions)
    unique_markets = len({p.market_slug or p.token_id for p in positions})
    if total_notional <= 0:
        return 0.0, unique_markets, 1.0

    market_notional: dict[str, float] = {}
    for position in positions:
        key = position.market_slug or position.token_id
        market_notional[key] = market_notional.get(key, 0.0) + max(0.0, position.notional)
    top_share = max(market_notional.values()) / total_notional if market_notional else 1.0
    return total_notional, unique_markets, top_share


def probe(base: str, path: str, method: str = "GET", params: dict[str, object] | None = None, timeout: float = 12.0) -> dict[str, object]:
    url = f"{base.rstrip('/')}{path}"
    start = time.perf_counter()
    try:
        with httpx.Client(timeout=timeout, follow_redirects=True) as client:
            response = client.request(method, url, params=params)
            ms = (time.perf_counter() - start) * 1000
            restricted, restricted_value = parse_restricted_cookie(response.headers)
            sample = response.text[:200].replace("\n", " ").replace("\r", " ")
            return {
                "ok": True,
                "url": url,
                "status": response.status_code,
                "reason": response.reason_phrase,
                "ms": round(ms, 1),
                "headers": {
                    "server": response.headers.get("server", ""),
                    "content_type": response.headers.get("content-type", ""),
                    "cf_ray": response.headers.get("cf-ray", ""),
                    "cache_control": response.headers.get("cache-control", ""),
                    "set_cookie": response.headers.get("set-cookie", ""),
                },
                "restricted": restricted,
                "restricted_raw": restricted_value,
                "sample": sample,
            }
    except Exception as exc:  # pragma: no cover - defensive for networking exceptions
        ms = (time.perf_counter() - start) * 1000
        return {
            "ok": False,
            "url": url,
            "status": 0,
            "reason": type(exc).__name__,
            "ms": round(ms, 1),
            "error": str(exc),
            "headers": {},
            "restricted": False,
            "restricted_raw": None,
            "sample": "",
        }


def evaluate_check(name: str, result: dict[str, object], expected: set[int] | None, allow401: bool = False) -> tuple[str, str]:
    if not result.get("ok"):
        return "FAIL", f"{name}: request failed ({result.get('reason')})"

    status = int(result["status"])
    if result.get("restricted"):
        return "BLOCK", f"{name}: geo-restricted marker detected"
    if status == 403 or status == 451:
        return "BLOCK", f"{name}: blocked by policy/status={status}"

    if expected is not None and status not in expected:
        if allow401 and status == 401:
            return "WARN", f"{name}: auth required (401, expected for private endpoint)"
        return "FAIL", f"{name}: unexpected status={status}"

    return "PASS", f"{name}: status={status}"


def run_history_smoke(
    *,
    data_api: str,
    clob_host: str,
    wallets: list[str],
    timeout: float,
    per_endpoint_limit: int,
    resolution_limit: int,
) -> list[dict[str, object]]:
    client = PolymarketDataClient(data_api, timeout_s=timeout, market_base_url=clob_host)
    scorer = SmartWalletScorer()
    results: list[dict[str, object]] = []
    try:
        for wallet in wallets:
            started = time.perf_counter()
            try:
                active_positions = client.get_active_positions(wallet, limit=max(1, per_endpoint_limit))
                trades = client.get_user_trades(wallet, limit=max(1, per_endpoint_limit), taker_only=False)
                activity = client.get_user_activity(
                    wallet,
                    limit=max(1, per_endpoint_limit),
                    types=["TRADE", "REDEEM"],
                )
                closed_positions = client.get_closed_positions(wallet, limit=min(50, max(1, per_endpoint_limit)))
                condition_slug_map = {
                    position.condition_id: position.market_slug
                    for position in closed_positions
                    if position.condition_id and position.market_slug
                }
                condition_ids = [
                    position.condition_id
                    for position in closed_positions
                    if position.condition_id
                ][: max(0, resolution_limit)]
                resolution_map = (
                    client.build_resolution_map(
                        set(condition_ids),
                        market_slugs=condition_slug_map,
                        max_pages=40,
                    )
                    if condition_ids
                    else {}
                )
                realized_metrics = build_realized_wallet_metrics(
                    closed_positions,
                    resolution_map if resolution_map else None,
                )

                wallet_score: float | None = None
                wallet_tier: str | None = None
                wallet_summary = ""
                if active_positions:
                    total_notional, unique_markets, top_share = summarize_positions(active_positions)
                    score = scorer.score_wallet(
                        total_notional_usd=total_notional,
                        active_positions=len(active_positions),
                        unique_markets=unique_markets,
                        top_market_share=top_share,
                        recent_activity_events=len(activity),
                        realized_metrics=realized_metrics if realized_metrics.closed_positions > 0 else None,
                    )
                    wallet_score = score.score
                    wallet_tier = score.tier
                    wallet_summary = score.summary

                ms = round((time.perf_counter() - started) * 1000, 1)
                has_data = bool(active_positions or trades or activity or closed_positions)
                if not has_data:
                    level = "WARN"
                    summary = f"history_wallet {wallet}: no recent data across active/trades/activity/closed"
                elif condition_ids and not resolution_map:
                    level = "WARN"
                    summary = (
                        f"history_wallet {wallet}: parsed data ok but no resolutions found "
                        f"for {len(condition_ids)} recent closed markets"
                    )
                else:
                    level = "PASS"
                    summary = (
                        f"history_wallet {wallet}: active={len(active_positions)} "
                        f"trades={len(trades)} activity={len(activity)} "
                        f"closed={len(closed_positions)} resolved={len(resolution_map)}"
                    )

                results.append(
                    {
                        "check": f"history_wallet:{wallet}",
                        "base": data_api,
                        "path": "history-smoke",
                        "status": 200,
                        "reason": "OK",
                        "level": level,
                        "summary": summary,
                        "cf_ray": "",
                        "restricted": False,
                        "restricted_raw": None,
                        "ms": ms,
                        "url": "",
                        "sample": wallet_summary[:200],
                        "wallet": wallet,
                        "active_positions": len(active_positions),
                        "trades": len(trades),
                        "activity": len(activity),
                        "closed_positions": len(closed_positions),
                        "resolved_markets": len(resolution_map),
                        "realized_metrics": realized_metrics.as_dict(),
                        "wallet_score": wallet_score,
                        "wallet_tier": wallet_tier,
                        "wallet_summary": wallet_summary,
                    }
                )
            except Exception as exc:  # pragma: no cover - defensive for networking exceptions
                ms = round((time.perf_counter() - started) * 1000, 1)
                results.append(
                    {
                        "check": f"history_wallet:{wallet}",
                        "base": data_api,
                        "path": "history-smoke",
                        "status": 0,
                        "reason": type(exc).__name__,
                        "level": "FAIL",
                        "summary": f"history_wallet {wallet}: request failed ({type(exc).__name__})",
                        "cf_ray": "",
                        "restricted": False,
                        "restricted_raw": None,
                        "ms": ms,
                        "url": "",
                        "sample": str(exc)[:200],
                        "wallet": wallet,
                    }
                )
    finally:
        client.close()
    return results


def main() -> int:
    env = load_env(Path(".env"))
    example_env = load_env(Path(".env.example"))
    parser = argparse.ArgumentParser(description="Polymarket network smoke test")
    parser.add_argument("--wallet", default=env.get("NETWORK_TEST_WALLET", ""), help="Wallet for positions endpoint smoke test")
    parser.add_argument(
        "--history-wallets",
        default=first_non_empty(
            env.get("NETWORK_TEST_WALLETS"),
            env.get("WATCH_WALLETS"),
            example_env.get("WATCH_WALLETS"),
        ),
        help="Comma-separated wallets for real history smoke checks",
    )
    parser.add_argument("--history-max-wallets", type=int, default=2, help="Max wallets to probe in history smoke")
    parser.add_argument("--history-limit", type=int, default=8, help="Per-endpoint limit for wallet history smoke")
    parser.add_argument("--resolution-limit", type=int, default=4, help="Max recent closed markets to resolve per wallet")
    parser.add_argument("--skip-history", action="store_true", help="Skip real wallet history smoke checks")
    parser.add_argument("--data-api", default=env.get("POLYMARKET_DATA_API", "https://data-api.polymarket.com"))
    parser.add_argument("--clob-host", default=env.get("POLYMARKET_CLOB_HOST", "https://clob.polymarket.com"))
    parser.add_argument("--log-path", default=os.getenv("NETWORK_SMOKE_LOG", "/tmp/poly_network_smoke.jsonl"))
    parser.add_argument("--timeout", type=float, default=12.0)
    args = parser.parse_args()

    wallet = args.wallet.strip().lower()
    if not WALLET_RE.match(wallet):
        fallback_wallets = parse_wallets(args.history_wallets)
        wallet = fallback_wallets[0] if fallback_wallets else "0x0000000000000000000000000000000000000000"
    history_wallets = parse_wallets(args.history_wallets)[: max(0, args.history_max_wallets)]

    checks = [
        ("polymarket_site", "https://polymarket.com", "/", "GET", None, False),
        ("data_api_root", args.data_api, "/", "GET", None, False),
        (
            "data_api_positions",
            args.data_api,
            "/positions",
            "GET",
            {"params": {"user": wallet, "sizeThreshold": 0, "limit": 1}, "expected": {200}},
            False,
        ),
        ("data_api_trades", args.data_api, "/trades", "GET", {"expected": {200}}, False),
        ("clob_root", args.clob_host, "/", "GET", {"expected": {200}}, False),
        ("clob_markets", args.clob_host, "/markets", "GET", {"expected": {200}}, False),
        ("clob_trades", args.clob_host, "/trades", "GET", {"expected": {200}}, True),
    ]

    print("==> Polymarket network smoke test")
    print(f"wallet={wallet}")
    print(f"data_api={args.data_api}")
    print(f"clob_host={args.clob_host}")
    if not args.skip_history:
        print(f"history_wallets={','.join(history_wallets) if history_wallets else '(none)'}")

    failures = 0
    blocks = 0
    warnings = 0

    per_check: list[dict[str, object]] = []
    for raw in checks:
        if len(raw) == 6:
            name, base, path, method, opts, allow401 = raw
        else:  # backward compatibility with old tuple shapes
            name, base, path, method, opts = raw
            allow401 = False

        expected = set(opts.get("expected", {200})) if opts else {200}
        params = opts.get("params") if opts else None

        result = probe(base, path, method=method, params=params, timeout=args.timeout)
        level, summary = evaluate_check(name, result, expected=expected, allow401=allow401)

        if level == "PASS":
            mark = "OK"
        elif level == "WARN":
            mark = "WARN"
            warnings += 1
        elif level == "BLOCK":
            mark = "BLOCK"
            blocks += 1
            failures += 1
        else:
            mark = "FAIL"
            failures += 1

        extra = (
            f"status={result.get('status')} cf_ray={result['headers'].get('cf_ray', '')}"
            f" restricted={result.get('restricted')}"
            f" ms={result.get('ms')}"
        )
        payload = {
            "check": name,
            "base": base,
            "path": path,
            "status": int(result.get("status") or 0),
            "reason": str(result.get("reason") or ""),
            "level": level,
            "summary": summary,
            "cf_ray": str(result["headers"].get("cf_ray", "")),
            "restricted": bool(result.get("restricted")),
            "restricted_raw": result.get("restricted_raw"),
            "ms": float(result.get("ms")),
            "url": str(result.get("url", "")),
            "sample": str(result.get("sample", "")),
        }
        per_check.append(payload)
        print(f"[{mark:5}] {summary} | {extra}")

    if not args.skip_history and history_wallets:
        print("==> Wallet history smoke")
        history_checks = run_history_smoke(
            data_api=args.data_api,
            clob_host=args.clob_host,
            wallets=history_wallets,
            timeout=args.timeout,
            per_endpoint_limit=max(1, args.history_limit),
            resolution_limit=max(0, args.resolution_limit),
        )
        for payload in history_checks:
            level = str(payload["level"])
            if level == "PASS":
                mark = "OK"
            elif level == "WARN":
                mark = "WARN"
                warnings += 1
            else:
                mark = "FAIL"
                failures += 1

            extra = (
                f"active={payload.get('active_positions', 0)} "
                f"trades={payload.get('trades', 0)} "
                f"closed={payload.get('closed_positions', 0)} "
                f"resolved={payload.get('resolved_markets', 0)} "
                f"ms={payload.get('ms')}"
            )
            per_check.append(payload)
            print(f"[{mark:5}] {payload['summary']} | {extra}")

    if blocks:
        print("BLOCK: network path appears geoblocked/restricted")
        exit_code = 2
    elif failures:
        print("FAIL: endpoint connectivity not aligned with expectation")
        exit_code = 1
    elif warnings:
        print(f"WARN: test passed with {warnings} warning(s) (non-fatal)")
        exit_code = 0
    else:
        print("PASS: Polymarket endpoints are reachable from this environment")
        exit_code = 0

    log_record = {
        "ts": int(time.time()),
        "wallet": wallet,
        "data_api": args.data_api,
        "clob_host": args.clob_host,
        "history_wallets": history_wallets,
        "checks": per_check,
        "summary": {
            "failures": failures,
            "blocks": blocks,
            "warnings": warnings,
            "exit_code": exit_code,
        },
    }
    log_path = Path(args.log_path)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(log_record, ensure_ascii=False))
        f.write("\n")

    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
