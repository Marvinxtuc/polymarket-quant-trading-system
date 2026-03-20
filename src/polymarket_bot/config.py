from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # Runtime
    poll_interval_seconds: int = Field(default=30, ge=5)
    dry_run: bool = True
    log_level: str = "INFO"
    decision_mode: str = "manual"
    candidate_db_path: str = "/tmp/poly_runtime_data/decision_terminal.db"
    candidate_ttl_seconds: int = Field(default=900, ge=60, le=86400)
    candidate_buy_small_fraction: float = Field(default=0.35, gt=0.0, le=1.0)
    candidate_buy_normal_fraction: float = Field(default=0.7, gt=0.0, le=1.0)
    candidate_follow_fraction: float = Field(default=1.0, gt=0.0, le=1.0)
    candidate_close_partial_fraction: float = Field(default=0.5, gt=0.0, le=1.0)
    candidate_auto_min_score: float = Field(default=72.0, ge=0.0, le=100.0)
    candidate_auto_min_wallet_score: float = Field(default=70.0, ge=0.0, le=100.0)
    candidate_buy_max_spread_pct: float = Field(default=120.0, ge=0.0, le=1000.0)
    candidate_buy_max_chase_pct: float = Field(default=12.0, ge=0.0, le=100.0)
    candidate_buy_spread_chase_guard_pct: float = Field(default=4.0, ge=0.0, le=100.0)
    candidate_notification_enabled: bool = False
    candidate_notification_min_score: float = Field(default=84.0, ge=0.0, le=100.0)
    candidate_notification_cooldown_seconds: int = Field(default=900, ge=30, le=86400)
    notify_local_enabled: bool = True
    notify_webhook_url: str = ""
    notify_webhook_urls: str = ""
    notify_telegram_bot_token: str = ""
    notify_telegram_chat_id: str = ""
    notify_telegram_api_base: str = "https://api.telegram.org"
    notify_telegram_parse_mode: str = ""
    notify_log_path: str = "/tmp/poly_runtime_data/notifier_events.jsonl"

    # Universe / strategy
    watch_wallets: str = ""
    wallet_discovery_enabled: bool = False
    wallet_discovery_mode: str = "union"
    wallet_discovery_paths: str = "/trades"
    wallet_discovery_limit: int = Field(default=300, ge=20, le=1000)
    wallet_discovery_top_n: int = Field(default=20, ge=5, le=500)
    wallet_discovery_min_events: int = Field(default=2, ge=1, le=50)
    wallet_discovery_refresh_seconds: int = Field(default=900, ge=60, le=86400)
    wallet_discovery_quality_bias_enabled: bool = True
    wallet_discovery_quality_top_n: int = Field(default=16, ge=1, le=100)
    wallet_discovery_history_bonus: float = Field(default=0.75, ge=0.0, le=3.0)
    wallet_discovery_topic_bonus: float = Field(default=0.5, ge=0.0, le=3.0)
    wallet_score_path: str = "/tmp/poly_runtime_data/wallet_scores.json"
    wallet_history_path: str = "/tmp/poly_runtime_data/wallet_history.json"
    wallet_history_refresh_seconds: int = Field(default=1800, ge=60, le=86400)
    wallet_history_max_wallets: int = Field(default=12, ge=1, le=100)
    wallet_history_closed_limit: int = Field(default=20, ge=1, le=50)
    wallet_history_resolution_limit: int = Field(default=8, ge=0, le=50)
    min_wallet_score: float = Field(default=50.0, ge=0.0, le=100.0)
    wallet_score_watch_multiplier: float = Field(default=0.4, gt=0.0, le=1.0)
    wallet_score_trade_multiplier: float = Field(default=0.75, gt=0.0, le=1.0)
    wallet_score_core_multiplier: float = Field(default=1.0, gt=0.0, le=1.0)
    topic_bias_enabled: bool = True
    topic_min_samples: int = Field(default=3, ge=1, le=20)
    topic_positive_roi: float = Field(default=0.08, ge=-1.0, le=5.0)
    topic_positive_win_rate: float = Field(default=0.6, ge=0.0, le=1.0)
    topic_negative_roi: float = Field(default=-0.02, ge=-1.0, le=5.0)
    topic_negative_win_rate: float = Field(default=0.45, ge=0.0, le=1.0)
    topic_boost_multiplier: float = Field(default=1.1, gt=0.0, le=1.5)
    topic_penalty_multiplier: float = Field(default=0.9, gt=0.0, le=1.0)
    wallet_exit_follow_enabled: bool = True
    min_wallet_decrease_usd: float = Field(default=200.0, ge=1.0)
    resonance_exit_enabled: bool = True
    resonance_min_wallets: int = Field(default=2, ge=2, le=6)
    resonance_min_wallet_score: float = Field(default=65.0, ge=0.0, le=100.0)
    resonance_trim_fraction: float = Field(default=0.35, gt=0.0, le=1.0)
    resonance_core_exit_fraction: float = Field(default=0.6, gt=0.0, le=1.0)
    wallet_signal_source: str = "hybrid"
    wallet_signal_lookback_seconds: int = Field(default=900, ge=60, le=86400)
    wallet_signal_page_size: int = Field(default=100, ge=10, le=500)
    wallet_signal_max_pages: int = Field(default=2, ge=1, le=10)
    min_wallet_increase_usd: float = Field(default=300.0, ge=1.0)
    max_signals_per_cycle: int = Field(default=3, ge=1)
    portfolio_netting_enabled: bool = True
    max_condition_exposure_pct: float = Field(default=0.015, gt=0.0, le=0.25)
    min_wallet_active_positions: int = Field(default=2, ge=1)
    min_wallet_unique_markets: int = Field(default=2, ge=1)
    min_wallet_total_notional_usd: float = Field(default=500.0, ge=0.0)
    max_wallet_top_market_share: float = Field(default=0.85, gt=0, le=1)
    stale_position_minutes: int = Field(default=20, ge=5, le=1440)
    stale_position_trim_pct: float = Field(default=0.4, gt=0, lt=1)
    stale_position_trim_cooldown_seconds: int = Field(default=900, ge=30, le=86400)
    stale_position_close_notional_usd: float = Field(default=10.0, ge=0.0)
    token_reentry_cooldown_seconds: int = Field(default=900, ge=0, le=86400)
    token_add_cooldown_seconds: int = Field(default=900, ge=0, le=86400)
    congested_utilization_threshold: float = Field(default=0.8, gt=0, le=1)
    congested_stale_minutes: int = Field(default=10, ge=1, le=1440)
    congested_trim_pct: float = Field(default=0.75, gt=0, lt=1)
    replay_entry_slippage_bps: float = Field(default=0.0, ge=0.0, le=500.0)
    replay_exit_slippage_bps: float = Field(default=0.0, ge=0.0, le=500.0)
    replay_taker_fee_bps: float = Field(default=0.0, ge=0.0, le=500.0)
    replay_entry_spread_multiplier: float = Field(default=0.0, ge=0.0, le=2.0)
    replay_exit_spread_multiplier: float = Field(default=0.0, ge=0.0, le=2.0)
    replay_edge_price_penalty_bps: float = Field(default=0.0, ge=0.0, le=200.0)
    replay_fee_keywords: str = "crypto,加密,ncaab,serie a,serie-a,seriea"

    # Risk
    bankroll_usd: float = Field(default=5000.0, ge=100.0)
    risk_per_trade_pct: float = Field(default=0.01, gt=0, le=0.05)
    daily_max_loss_pct: float = Field(default=0.03, gt=0, le=0.2)
    max_open_positions: int = Field(default=8, ge=1)
    min_price: float = Field(default=0.08, ge=0.01, le=0.999)
    max_price: float = Field(default=0.92, ge=0.01, le=0.999)
    control_path: str = "/tmp/poly_runtime_data/control.json"

    # APIs
    polymarket_data_api: str = "https://data-api.polymarket.com"
    polymarket_clob_host: str = "https://clob.polymarket.com"
    runtime_state_path: str = "/tmp/poly_runtime_data/runtime_state.json"
    event_log_path: str = "/tmp/poly_runtime_data/events.ndjson"
    ledger_path: str = "/tmp/poly_runtime_data/ledger.jsonl"
    runtime_reconcile_interval_seconds: int = Field(default=180, ge=60, le=3600)
    account_sync_refresh_seconds: int = Field(default=300, ge=60, le=3600)
    order_dedup_ttl_seconds: int = Field(default=120, ge=1, le=3600)
    pending_order_timeout_seconds: int = Field(default=1800, ge=60, le=86400)
    user_stream_enabled: bool = True
    user_stream_url: str = "wss://ws-subscriptions-clob.polymarket.com/ws/user"
    user_stream_ping_interval_seconds: int = Field(default=10, ge=5, le=120)
    user_stream_reconnect_seconds: int = Field(default=5, ge=1, le=120)
    user_stream_buffer_size: int = Field(default=1000, ge=100, le=10000)

    # Live admission gate
    live_network_smoke_max_age_seconds: int = Field(default=43200, ge=60, le=604800)
    live_allowance_ready: bool = False
    live_geoblock_ready: bool = False
    live_account_ready: bool = False

    # Optional live-trading auth
    chain_id: int = 137
    clob_signature_type: int = Field(default=0, ge=0, le=2)
    private_key: str = ""
    funder_address: str = ""

    @property
    def wallet_list(self) -> list[str]:
        return [x.strip().lower() for x in self.watch_wallets.split(",") if x.strip()]

    @property
    def wallet_discovery_path_list(self) -> list[str]:
        values: list[str] = []
        for raw in self.wallet_discovery_paths.split(","):
            value = raw.strip()
            if not value:
                continue
            if not value.startswith("/"):
                value = f"/{value}"
            values.append(value)
        return values

    @property
    def replay_fee_keyword_list(self) -> list[str]:
        return [value.strip().lower() for value in self.replay_fee_keywords.split(",") if value.strip()]

    @property
    def notify_webhook_url_list(self) -> list[str]:
        values: list[str] = []
        for raw in (self.notify_webhook_url, self.notify_webhook_urls):
            for chunk in raw.replace("\n", ",").replace(";", ",").split(","):
                value = chunk.strip()
                if value and value not in values:
                    values.append(value)
        return values

    @property
    def notify_telegram_enabled(self) -> bool:
        return bool(self.notify_telegram_bot_token and self.notify_telegram_chat_id)
