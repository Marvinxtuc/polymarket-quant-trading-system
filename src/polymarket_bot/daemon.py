from __future__ import annotations

import argparse
import json
import logging
import os
import time
from typing import Any

from polymarket_bot.config import Settings
from polymarket_bot.main import build_trader, setup_logger


def _safe_write_json(path: str, payload: dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp_path = f"{path}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False)
    os.replace(tmp_path, path)


def _fmt_ago(seconds: int) -> str:
    if seconds < 60:
        return f"{seconds}s前"
    return f"{max(1, seconds // 60)}m前"


def _build_state(trader, settings: Settings) -> dict[str, Any]:
    now = int(time.time())
    wallet_metrics = trader.strategy.latest_wallet_metrics()
    wallets_sorted = sorted(
        wallet_metrics.items(),
        key=lambda item: float(item[1].get("total_notional", 0.0)),
        reverse=True,
    )

    wallets = []
    for wallet, metrics in wallets_sorted[:8]:
        wallets.append(
            {
                "wallet": wallet,
                "notional": float(metrics.get("total_notional", 0.0)),
                "positions": int(metrics.get("positions", 0)),
                "unique_markets": int(metrics.get("unique_markets", 0)),
                "top_market_share": float(metrics.get("top_market_share", 0.0)),
                "daily_pnl": 0.0,
                "status": "在线",
                "note": "-",
            }
        )

    sources = []
    total_notional = sum(max(0.0, float(m.get("total_notional", 0.0))) for _, m in wallets_sorted[:8])
    for wallet, metrics in wallets_sorted[:8]:
        wallet_notional = max(0.0, float(metrics.get("total_notional", 0.0)))
        weight = 0.0 if total_notional <= 0 else wallet_notional / total_notional
        sources.append(
            {
                "name": wallet,
                "weight": round(weight, 2),
                "status": "在线",
                "updated": _fmt_ago(int(max(0, now - trader._cached_wallets_ts if trader._cached_wallets_ts else 0))),
                "hit_rate": "-",
            }
        )

    positions = []
    for pos in list(trader.positions_book.values())[:8]:
        notional = float(pos.get("notional") or 0.0)
        unrealized = float(pos.get("unrealized_pnl") or 0.0)
        edge_pct = 0.0 if notional <= 0 else (unrealized / notional * 100.0)
        positions.append(
            {
                "title": str(pos.get("market_slug") or pos.get("token_id") or "-"),
                "market_slug": str(pos.get("market_slug") or ""),
                "outcome": str(pos.get("outcome") or "YES"),
                "quantity": float(pos.get("quantity") or 0.0),
                "notional": notional,
                "unrealized_pnl": unrealized,
                "edge_pct": edge_pct,
                "opened_ts": int(pos.get("opened_ts") or now),
                "reason": "wallet follower",
                "exit_rule": "time-exit/cooldown",
            }
        )

    orders = list(trader.recent_orders)[:20]
    timeline = [
        {"time": time.strftime("%H:%M", time.localtime(int(o.get("ts", now)))), "text": f"signal {o.get('side')} {o.get('title')}"}
        for o in orders[:8]
    ]

    alerts: list[dict[str, str]] = []
    if not trader.last_wallets:
        alerts.append({"cls": "yellow", "tag": "关注", "message": "未解析到监控钱包，检查 discovery 配置或网络"})
    if any(str(o.get("status")) == "REJECTED" for o in orders[:5]):
        alerts.append({"cls": "red", "tag": "处理", "message": "最近存在下单失败，请检查风控与流动性"})
    if not alerts:
        alerts.append({"cls": "green", "tag": "正常", "message": "系统运行正常，数据持续更新"})

    exposure_pct = 0.0
    if settings.max_open_positions > 0:
        exposure_pct = min(100.0, trader.state.open_positions / settings.max_open_positions * 100.0)
    per_trade_notional = settings.bankroll_usd * settings.risk_per_trade_pct
    daily_loss_budget_usd = settings.bankroll_usd * settings.daily_max_loss_pct
    daily_loss_used_usd = min(daily_loss_budget_usd, max(0.0, -trader.state.daily_realized_pnl))
    daily_loss_used_pct = 0.0 if daily_loss_budget_usd <= 0 else (daily_loss_used_usd / daily_loss_budget_usd * 100.0)
    daily_loss_remaining_pct = max(0.0, 100.0 - daily_loss_used_pct)
    est_openings = 0 if per_trade_notional <= 0 else int((daily_loss_budget_usd - daily_loss_used_usd) / per_trade_notional)

    return {
        "ts": now,
        "config": {
            "poll_interval_seconds": int(settings.poll_interval_seconds),
            "bankroll_usd": float(settings.bankroll_usd),
            "risk_per_trade_pct": float(settings.risk_per_trade_pct),
            "daily_max_loss_pct": float(settings.daily_max_loss_pct),
            "max_open_positions": int(settings.max_open_positions),
            "min_wallet_increase_usd": float(settings.min_wallet_increase_usd),
            "max_signals_per_cycle": int(settings.max_signals_per_cycle),
            "wallet_pool_size": int(len(trader.last_wallets)),
            "token_add_cooldown_seconds": int(settings.token_add_cooldown_seconds),
            "token_reentry_cooldown_seconds": int(settings.token_reentry_cooldown_seconds),
            "stale_position_minutes": int(settings.stale_position_minutes),
            "stale_position_trim_pct": float(settings.stale_position_trim_pct),
            "stale_position_trim_cooldown_seconds": int(settings.stale_position_trim_cooldown_seconds),
            "stale_position_close_notional_usd": float(settings.stale_position_close_notional_usd),
            "congested_utilization_threshold": float(settings.congested_utilization_threshold),
            "congested_stale_minutes": int(settings.congested_stale_minutes),
            "congested_trim_pct": float(settings.congested_trim_pct),
            "min_price": float(settings.min_price),
            "max_price": float(settings.max_price),
            "wallet_discovery_enabled": bool(settings.wallet_discovery_enabled),
            "wallet_discovery_mode": str(settings.wallet_discovery_mode),
        },
        "summary": {
            "pnl_today": float(trader.state.daily_realized_pnl),
            "equity": float(settings.bankroll_usd + trader.state.daily_realized_pnl),
            "open_positions": int(trader.state.open_positions),
            "max_open_positions": int(settings.max_open_positions),
            "exposure_pct": float(round(exposure_pct, 2)),
            "signals": int(len(trader.last_signals)),
            "per_trade_notional": float(per_trade_notional),
            "daily_loss_budget_usd": float(daily_loss_budget_usd),
            "daily_loss_used_pct": float(round(daily_loss_used_pct, 2)),
            "daily_loss_remaining_pct": float(round(daily_loss_remaining_pct, 2)),
            "est_openings": int(max(0, est_openings)),
        },
        "positions": positions,
        "orders": orders,
        "wallets": wallets,
        "sources": sources,
        "alerts": alerts,
        "timeline": timeline,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Polymarket runtime daemon")
    parser.add_argument("--state-path", default="/tmp/poly_runtime_data/state.json")
    parser.add_argument("--once", action="store_true", help="Run one cycle and exit")
    args = parser.parse_args()

    settings = Settings()
    setup_logger(settings.log_level)
    log = logging.getLogger("polybot.daemon")
    trader = build_trader(settings)
    try:
        while True:
            trader.step()
            payload = _build_state(trader, settings)
            _safe_write_json(args.state_path, payload)
            log.info(
                "state_updated wallets=%d signals=%d orders=%d",
                len(trader.last_wallets),
                len(trader.last_signals),
                len(trader.recent_orders),
            )
            if args.once:
                return
            time.sleep(settings.poll_interval_seconds)
    finally:
        trader.data_client.close()


if __name__ == "__main__":
    main()
