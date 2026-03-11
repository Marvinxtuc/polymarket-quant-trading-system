from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # Runtime
    poll_interval_seconds: int = Field(default=45, ge=5)
    dry_run: bool = True
    log_level: str = "INFO"

    # Universe / strategy
    watch_wallets: str = ""
    wallet_discovery_enabled: bool = True
    wallet_discovery_mode: str = "union"
    wallet_discovery_paths: str = "/trades"
    wallet_discovery_limit: int = Field(default=300, ge=20, le=1000)
    wallet_discovery_top_n: int = Field(default=50, ge=5, le=500)
    wallet_discovery_min_events: int = Field(default=2, ge=1, le=50)
    wallet_discovery_refresh_seconds: int = Field(default=900, ge=60, le=86400)
    min_wallet_increase_usd: float = Field(default=300.0, ge=1.0)
    max_signals_per_cycle: int = Field(default=5, ge=1)
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

    # Risk
    bankroll_usd: float = Field(default=5000.0, ge=100.0)
    risk_per_trade_pct: float = Field(default=0.01, gt=0, le=0.05)
    daily_max_loss_pct: float = Field(default=0.03, gt=0, le=0.2)
    max_open_positions: int = Field(default=8, ge=1)
    min_price: float = Field(default=0.08, ge=0.01, le=0.99)
    max_price: float = Field(default=0.92, ge=0.01, le=0.99)

    # APIs
    polymarket_data_api: str = "https://data-api.polymarket.com"
    polymarket_clob_host: str = "https://clob.polymarket.com"

    # Optional live-trading auth
    chain_id: int = 137
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
