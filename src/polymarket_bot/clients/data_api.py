from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential


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


class PolymarketDataClient:
    def __init__(self, base_url: str, timeout_s: float = 15.0) -> None:
        self.base_url = base_url.rstrip("/")
        self._client = httpx.Client(timeout=timeout_s)

    def close(self) -> None:
        self._client.close()

    @retry(wait=wait_exponential(multiplier=0.5, min=0.5, max=4), stop=stop_after_attempt(4))
    def _get_json(self, path: str, params: dict[str, Any] | None = None) -> Any:
        url = f"{self.base_url}{path}"
        r = self._client.get(url, params=params)
        r.raise_for_status()
        return r.json()

    def get_active_positions(self, wallet: str, limit: int = 200) -> list[Position]:
        data = self._get_json(
            "/positions",
            params={"user": wallet, "sizeThreshold": 0, "limit": limit},
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

            size = float(row.get("size") or 0)
            if size <= 0:
                continue

            avg_price = float(row.get("avgPrice") or row.get("curPrice") or 0.0)
            if avg_price <= 0:
                avg_price = 0.5

            notional = size * avg_price
            positions.append(
                Position(
                    wallet=wallet,
                    token_id=token_id,
                    market_slug=str(row.get("slug") or ""),
                    outcome=str(row.get("outcome") or ""),
                    avg_price=avg_price,
                    size=size,
                    notional=notional,
                    timestamp=int(row.get("timestamp") or now_ts),
                )
            )
        return positions

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
