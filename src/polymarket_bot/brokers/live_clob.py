from __future__ import annotations

from polymarket_bot.brokers.base import Broker
from polymarket_bot.types import ExecutionResult, Signal


class LiveClobBroker(Broker):
    def __init__(self, host: str, chain_id: int, private_key: str, funder: str) -> None:
        try:
            from py_clob_client.client import ClobClient
            from py_clob_client.clob_types import OrderArgs, OrderType
            from py_clob_client.order_builder.constants import BUY
        except Exception as exc:  # pragma: no cover
            raise RuntimeError(
                "py-clob-client not installed. Install with: pip install '.[live]'"
            ) from exc

        self._OrderArgs = OrderArgs
        self._OrderType = OrderType
        self._BUY = BUY

        self.client = ClobClient(host, key=private_key, chain_id=chain_id, signature_type=1, funder=funder)
        creds = self.client.create_or_derive_api_creds()
        self.client.set_api_creds(creds)

    def execute(self, signal: Signal, notional_usd: float) -> ExecutionResult:
        price = max(0.01, min(0.99, signal.price_hint))
        size = notional_usd / price

        order_args = self._OrderArgs(
            token_id=signal.token_id,
            price=price,
            size=size,
            side=self._BUY,
        )

        signed = self.client.create_order(order_args)
        resp = self.client.post_order(signed, self._OrderType.GTC)

        order_id = None
        if isinstance(resp, dict):
            order_id = str(resp.get("orderID") or resp.get("id") or "") or None

        return ExecutionResult(
            ok=True,
            broker_order_id=order_id,
            message="live order posted",
            filled_notional=notional_usd,
            filled_price=price,
        )
