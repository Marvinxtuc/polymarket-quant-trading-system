from __future__ import annotations

import argparse
import logging

from polymarket_bot.brokers.live_clob import LiveClobBroker
from polymarket_bot.brokers.paper import PaperBroker
from polymarket_bot.clients.data_api import PolymarketDataClient
from polymarket_bot.config import Settings
from polymarket_bot.i18n import t as i18n_t
from polymarket_bot.risk import RiskManager
from polymarket_bot.runner import Trader
from polymarket_bot.strategies.wallet_follower import WalletFollowerStrategy


def _main_t(key: str, params: dict[str, object] | None = None, *, fallback: str = "") -> str:
    return i18n_t(f"cli.main.{key}", dict(params or {}), fallback=fallback)


def setup_logger(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    # Keep operator-facing logs focused on trading decisions.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)


def build_trader(settings: Settings) -> Trader:
    data_client = PolymarketDataClient(
        settings.polymarket_data_api,
        market_base_url=settings.polymarket_clob_host,
    )
    strategy = WalletFollowerStrategy(
        client=data_client,
        min_increase_usd=settings.min_wallet_increase_usd,
        max_signals_per_cycle=settings.max_signals_per_cycle,
        min_active_positions=settings.min_wallet_active_positions,
        min_unique_markets=settings.min_wallet_unique_markets,
        min_total_notional_usd=settings.min_wallet_total_notional_usd,
        max_top_market_share=settings.max_wallet_top_market_share,
        min_wallet_score=settings.min_wallet_score,
        min_decrease_usd=settings.min_wallet_decrease_usd,
        follow_wallet_exits=settings.wallet_exit_follow_enabled,
        resonance_exit_enabled=settings.resonance_exit_enabled,
        resonance_min_wallets=settings.resonance_min_wallets,
        resonance_min_wallet_score=settings.resonance_min_wallet_score,
        resonance_trim_fraction=settings.resonance_trim_fraction,
        resonance_core_exit_fraction=settings.resonance_core_exit_fraction,
        signal_source=settings.wallet_signal_source,
        signal_lookback_seconds=settings.wallet_signal_lookback_seconds,
        signal_page_size=settings.wallet_signal_page_size,
        signal_max_pages=settings.wallet_signal_max_pages,
        live_buy_max_chase_pct=settings.candidate_buy_max_chase_pct,
    )
    risk = RiskManager(settings)

    if settings.dry_run:
        broker = PaperBroker(settings=settings)
    else:
        if not settings.private_key or not settings.funder_address:
            raise RuntimeError(
                _main_t("runtime.liveRequiresSecrets", fallback="LIVE mode requires PRIVATE_KEY and FUNDER_ADDRESS")
            )
        broker = LiveClobBroker(
            host=settings.polymarket_clob_host,
            chain_id=settings.chain_id,
            private_key=settings.private_key,
            funder=settings.funder_address,
            market_client=data_client,
            signature_type=settings.clob_signature_type,
            user_stream_enabled=settings.user_stream_enabled,
            user_stream_url=settings.user_stream_url,
            user_stream_ping_interval_seconds=settings.user_stream_ping_interval_seconds,
            user_stream_reconnect_seconds=settings.user_stream_reconnect_seconds,
            user_stream_buffer_size=settings.user_stream_buffer_size,
        )

    return Trader(
        settings=settings,
        data_client=data_client,
        strategy=strategy,
        risk=risk,
        broker=broker,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=_main_t("description", fallback="Polymarket automated trader"))
    parser.add_argument("--once", action="store_true", help=_main_t("once", fallback="Run one cycle and exit"))
    args = parser.parse_args()

    settings = Settings()
    setup_logger(settings.log_level)

    trader = build_trader(settings)
    try:
        trader.run(once=args.once)
    finally:
        trader.broker.close()
        trader.data_client.close()


if __name__ == "__main__":
    main()
