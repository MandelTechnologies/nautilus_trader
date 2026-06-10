# -------------------------------------------------------------------------------------------------
#  QuantChat Local Paper Trading Adapter for Nautilus Trader
#  https://github.com/mandeltechnologies/quantchat.com
# -------------------------------------------------------------------------------------------------

from __future__ import annotations

from dataclasses import dataclass
from decimal import ROUND_HALF_UP
from decimal import Decimal
import random

from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity


@dataclass
class FillResult:
    """
    Result of a simulated fill.
    """

    fill_price: Price
    fill_qty: Quantity
    latency_ms: int


class QuantChatFillModel:
    """
    Fill model for simulating order execution.

    Applies fixed basis-point price slippage in the direction unfavorable to the
    order, plus execution latency (base + random jitter). The slippage semantic
    matches backtests, where the same basis points are charged as taker commission:
    one ``slippageBps`` setting costs the same in both runtimes.

    Parameters
    ----------
    base_latency_ms : int, default 50
        Base execution latency in milliseconds.
    latency_jitter_ms : int, default 20
        Random jitter added to base latency (0 to jitter_ms).
    slippage_bps : float, default 5.0
        Price slippage in basis points of the market price.

    """

    def __init__(
        self,
        base_latency_ms: int = 50,
        latency_jitter_ms: int = 20,
        slippage_bps: float = 5.0,
    ) -> None:
        self.base_latency_ms = base_latency_ms
        self.latency_jitter_ms = latency_jitter_ms
        self.slippage_bps = slippage_bps

    def simulate_fill(
        self,
        order_side: OrderSide,
        quantity: Quantity,
        market_price: Price,
    ) -> FillResult:
        """
        Simulate a fill for the given order parameters.

        Parameters
        ----------
        order_side : OrderSide
            The side of the order (BUY or SELL).
        quantity : Quantity
            The order quantity.
        market_price : Price
            The current market price.

        Returns
        -------
        FillResult
            The simulated fill result with price, quantity, and latency.

        """
        # S311: random is for simulation, not cryptography
        latency_ms = self.base_latency_ms + random.randint(0, self.latency_jitter_ms)  # noqa: S311

        slippage = Decimal(str(self.slippage_bps)) / Decimal("10000")
        if order_side == OrderSide.BUY:
            slipped = market_price.as_decimal() * (Decimal("1") + slippage)
        else:
            slipped = market_price.as_decimal() * (Decimal("1") - slippage)

        quantum = Decimal(1).scaleb(-int(market_price.precision))
        fill_price = Price.from_str(str(slipped.quantize(quantum, rounding=ROUND_HALF_UP)))

        return FillResult(
            fill_price=fill_price,
            fill_qty=quantity,
            latency_ms=latency_ms,
        )
