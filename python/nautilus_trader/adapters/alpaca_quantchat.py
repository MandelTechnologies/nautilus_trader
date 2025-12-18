"""
Lightweight Alpaca adapter scaffold for QuantChat Nautilus workers.

This is a placeholder for live execution; methods must be implemented before production
use.

"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class AlpacaAuth:
    api_key: str
    api_secret: str
    paper: bool = True
    base_url: str | None = None


@dataclass
class RiskLimits:
    max_orders_per_minute: int
    max_notional_usd: float | None = None
    allowed_symbols: list[str] | None = None


class AlpacaQuantChatAdapter:
    """
    Placeholder adapter; wire to Alpaca REST/WS and emit Nautilus events.
    """

    def __init__(self, auth: AlpacaAuth, risk: RiskLimits) -> None:
        self.auth = auth
        self.risk = risk

    # --- Lifecycle -----------------------------------------------------
    async def start(self) -> None:
        raise NotImplementedError("start() not implemented")

    async def stop(self) -> None:
        raise NotImplementedError("stop() not implemented")

    # --- Order flow ----------------------------------------------------
    async def submit_order(
        self,
        symbol: str,
        side: str,
        quantity: float,
        order_type: str,
        tif: str | None = None,
        limit_price: float | None = None,
        stop_price: float | None = None,
        client_order_id: str | None = None,
    ) -> dict:
        """
        Enforce risk limits then forward to Alpaca.
        """
        raise NotImplementedError("submit_order() not implemented")

    async def cancel_order(self, order_id: str) -> dict:
        raise NotImplementedError("cancel_order() not implemented")

    # --- State / snapshots ---------------------------------------------
    async def fetch_positions(self) -> list[dict]:
        raise NotImplementedError("fetch_positions() not implemented")

    async def fetch_cash(self) -> dict:
        raise NotImplementedError("fetch_cash() not implemented")

    async def poll_health(self) -> dict:
        raise NotImplementedError("poll_health() not implemented")
