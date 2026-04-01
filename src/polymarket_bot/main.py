from __future__ import annotations

import argparse
import logging

from polymarket_bot.brokers.live_clob import LiveClobBroker
from polymarket_bot.brokers.paper import PaperBroker
from polymarket_bot.clients.data_api import PolymarketDataClient
from polymarket_bot.config import Settings
from polymarket_bot.i18n import t as i18n_t
from polymarket_bot.locks import (
    FileLock,
    SINGLE_WRITER_CONFLICT_EXIT_CODE,
    SINGLE_WRITER_CONFLICT_REASON,
    SingleWriterLockError,
    derive_writer_scope,
)
from polymarket_bot.risk import RiskManager
from polymarket_bot.runner import Trader
from polymarket_bot.secrets import SecretConfigurationError, resolve_live_secret_bundle
from polymarket_bot.signer_client import SignerClientError, build_signer_client
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


def build_trader(settings: Settings, *, pre_acquired_writer_lock: FileLock | None = None) -> Trader:
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
        try:
            live_secrets = resolve_live_secret_bundle(settings)
        except SecretConfigurationError as exc:
            raise RuntimeError(
                _main_t(
                    "runtime.liveSignerSecretBoundaryInvalid",
                    {"reasonCode": str(exc.reason_code)},
                    fallback=f"LIVE mode secret boundary invalid ({exc.reason_code})",
                )
            ) from exc
        try:
            signer_client = build_signer_client(live_secrets)
            signer_health = signer_client.health_check()
        except SignerClientError as exc:
            raise RuntimeError(
                _main_t(
                    "runtime.liveSignerUnavailable",
                    {"reasonCode": str(exc.reason_code)},
                    fallback=f"LIVE mode signer unavailable ({exc.reason_code})",
                )
            ) from exc

        if not bool(signer_health.healthy):
            reason_code = str(signer_health.reason_code or "signer_unhealthy")
            raise RuntimeError(
                _main_t(
                    "runtime.liveSignerUnhealthy",
                    {"reasonCode": reason_code},
                    fallback=f"LIVE mode signer unhealthy ({reason_code})",
                )
            )

        broker = LiveClobBroker(
            host=settings.polymarket_clob_host,
            chain_id=settings.chain_id,
            funder=live_secrets.funder_address,
            signer_client=signer_client,
            signer_health=signer_health,
            api_key=live_secrets.clob_api_key,
            api_secret=live_secrets.clob_api_secret,
            api_passphrase=live_secrets.clob_api_passphrase,
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
        pre_acquired_writer_lock=pre_acquired_writer_lock,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=_main_t("description", fallback="Polymarket automated trader"))
    parser.add_argument("--once", action="store_true", help=_main_t("once", fallback="Run one cycle and exit"))
    args = parser.parse_args()

    settings = Settings()
    setup_logger(settings.log_level)
    log = logging.getLogger("polybot.main")
    writer_scope = derive_writer_scope(
        dry_run=bool(settings.dry_run),
        funder_address=str(settings.funder_address or ""),
        watch_wallets=str(settings.watch_wallets or ""),
    )
    log.info(
        "SINGLE_WRITER_SCOPE scope=%s dry_run=%s lock_path=%s",
        writer_scope,
        bool(settings.dry_run),
        settings.wallet_lock_path,
    )
    if not bool(settings.dry_run) and not bool(settings.enable_single_writer):
        log.error(
            "startup blocked reason_code=single_writer_required_live dry_run=%s enable_single_writer=%s scope=%s",
            bool(settings.dry_run),
            bool(settings.enable_single_writer),
            writer_scope,
        )
        raise SystemExit(2)

    pre_acquired_lock: FileLock | None = None
    if bool(settings.enable_single_writer):
        pre_acquired_lock = FileLock(
            settings.wallet_lock_path,
            timeout=0.0,
            writer_scope=writer_scope,
        )
        try:
            pre_acquired_lock.acquire()
        except SingleWriterLockError as exc:
            reason_code = str(getattr(exc, "reason_code", "") or "")
            log.error(
                "startup blocked reason_code=%s scope=%s lock_path=%s err=%s",
                reason_code,
                writer_scope,
                settings.wallet_lock_path,
                exc,
            )
            if reason_code == SINGLE_WRITER_CONFLICT_REASON:
                raise SystemExit(SINGLE_WRITER_CONFLICT_EXIT_CODE)
            raise SystemExit(3)

    trader = None
    try:
        trader = build_trader(settings, pre_acquired_writer_lock=pre_acquired_lock)
        trader.run(once=args.once)
    finally:
        if trader is not None:
            trader.broker.close()
            trader.data_client.close()
            if getattr(trader, "_writer_lock", None) is not None:
                trader._writer_lock.release()
                trader._writer_lock = None
        elif pre_acquired_lock is not None:
            pre_acquired_lock.release()


if __name__ == "__main__":
    main()
