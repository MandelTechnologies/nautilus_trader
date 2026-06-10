# -------------------------------------------------------------------------------------------------
#  QuantChat Local Paper Trading Adapter for Nautilus Trader
#  https://github.com/mandeltechnologies/quantchat.com
# -------------------------------------------------------------------------------------------------

from __future__ import annotations

import asyncio
from datetime import datetime
from decimal import Decimal
import json

from nautilus_trader.adapters.quantchat.config import QuantChatExecClientConfig
from nautilus_trader.adapters.quantchat.constants import QUANTCHAT_VENUE
from nautilus_trader.adapters.quantchat.constants import REDIS_BAR_CHANNEL_PREFIX
from nautilus_trader.adapters.quantchat.constants import bar_channel
from nautilus_trader.adapters.quantchat.fill_model import QuantChatFillModel
from nautilus_trader.adapters.quantchat.providers import QuantChatInstrumentProvider
from nautilus_trader.adapters.quantchat.pubsub import ResilientPubSub
from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.component import MessageBus
from nautilus_trader.common.enums import LogColor
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.execution.messages import CancelOrder
from nautilus_trader.execution.messages import GenerateFillReports
from nautilus_trader.execution.messages import GenerateOrderStatusReports
from nautilus_trader.execution.messages import GeneratePositionStatusReports
from nautilus_trader.execution.messages import ModifyOrder
from nautilus_trader.execution.messages import SubmitOrder
from nautilus_trader.execution.reports import FillReport
from nautilus_trader.execution.reports import OrderStatusReport
from nautilus_trader.execution.reports import PositionStatusReport
from nautilus_trader.live.execution_client import LiveExecutionClient
from nautilus_trader.model.enums import AccountType
from nautilus_trader.model.enums import LiquiditySide
from nautilus_trader.model.enums import OmsType
from nautilus_trader.model.enums import OrderType
from nautilus_trader.model.identifiers import AccountId
from nautilus_trader.model.identifiers import ClientId
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import TradeId
from nautilus_trader.model.identifiers import VenueOrderId
from nautilus_trader.model.objects import AccountBalance
from nautilus_trader.model.objects import Currency
from nautilus_trader.model.objects import Money
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity


class QuantChatExecutionClient(LiveExecutionClient):
    """
    Provides an execution client for QuantChat local paper trading.

    Simulates order execution with configurable slippage and latency.
    Uses Redis to receive price data for fill simulation.

    Parameters
    ----------
    loop : asyncio.AbstractEventLoop
        The event loop for the client.
    msgbus : MessageBus
        The message bus for the client.
    cache : Cache
        The cache for the client.
    clock : LiveClock
        The clock for the client.
    instrument_provider : QuantChatInstrumentProvider
        The instrument provider.
    config : QuantChatExecClientConfig
        The configuration for the client.
    name : str, optional
        The custom client ID.

    """

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        msgbus: MessageBus,
        cache: Cache,
        clock: LiveClock,
        instrument_provider: QuantChatInstrumentProvider,
        config: QuantChatExecClientConfig,
        name: str | None = None,
    ) -> None:
        super().__init__(
            loop=loop,
            client_id=ClientId(name or "QUANTCHAT"),
            venue=QUANTCHAT_VENUE,
            oms_type=OmsType.NETTING,
            instrument_provider=instrument_provider,
            account_type=AccountType.CASH,
            base_currency=Currency.from_str("USD"),
            msgbus=msgbus,
            cache=cache,
            clock=clock,
        )

        self._config = config
        self._redis_url = config.redis_url

        # Fill model for simulating realistic execution
        self._fill_model = QuantChatFillModel(
            base_latency_ms=config.base_latency_ms,
            slippage_bps=config.slippage_bps,
            partial_fill_prob=config.partial_fill_prob,
        )

        # Redis pub/sub for price data
        self._pubsub: ResilientPubSub | None = None

        # Latest close per symbol for fill simulation, with the bar timestamp it came
        # from so a slower aggregate channel never overwrites a fresher price.
        self._latest_prices: dict[str, Decimal] = {}
        self._latest_price_ts: dict[str, int] = {}

        # Track subscribed symbols
        self._subscribed_symbols: set[str] = set()

        # Register a paper account before emitting any account events.
        self._set_account_id(AccountId(f"QUANTCHAT-PAPER-{UUID4().value[:8]}"))

        # Pending orders (for limit/stop orders - future enhancement)
        self._pending_orders: dict[str, SubmitOrder] = {}

    def _parse_starting_balance(self) -> list[AccountBalance]:
        """
        Parse the starting_balance config string into AccountBalance objects.
        """
        # Format: "100000 USD" or "100000 USD, 1.5 BTC"
        balances = []
        parts = self._config.starting_balance.split(",")

        for part in parts:
            part = part.strip()
            if not part:
                continue
            tokens = part.split()
            if len(tokens) != 2:
                self._log.warning(f"Invalid balance format: {part}, expected 'AMOUNT CURRENCY'")
                continue

            amount_str, currency_str = tokens
            try:
                currency = Currency.from_str(currency_str.upper())
                money = Money.from_str(f"{amount_str} {currency_str.upper()}")
                balances.append(
                    AccountBalance(
                        total=money,
                        locked=Money(0, currency),
                        free=money,
                    ),
                )
            except Exception as e:
                self._log.warning(f"Failed to parse balance '{part}': {e}")

        return balances

    async def _connect(self) -> None:
        """
        Connect the execution client.
        """
        await self._instrument_provider.initialize()

        self._pubsub = ResilientPubSub(self._redis_url, self._handle_price_update, self._log)
        await self._pubsub.start()

        # Initialize account state with starting balance
        balances = self._parse_starting_balance()
        if balances:
            self.generate_account_state(
                balances=balances,
                margins=[],
                reported=True,
                ts_event=self._clock.timestamp_ns(),
            )
            self._log.info(f"Initialized account with balances: {self._config.starting_balance}")

        self._log.info(
            f"QuantChat execution client connected (account: {self.account_id})",
            LogColor.GREEN,
        )

    async def _disconnect(self) -> None:
        """
        Disconnect the execution client.
        """
        if self._pubsub:
            await self._pubsub.stop()
            self._pubsub = None

        self._latest_prices.clear()
        self._latest_price_ts.clear()
        self._subscribed_symbols.clear()
        self._pending_orders.clear()

        self._log.info("QuantChat execution client disconnected")

    def _handle_price_update(self, channel: str, data: str) -> None:
        """
        Handle an incoming bar payload, keeping the freshest close per symbol.
        """
        if not channel.startswith(REDIS_BAR_CHANNEL_PREFIX):
            return

        payload = json.loads(data)
        # Channel format: market:bar:{symbol}:{timeframe}
        symbol = channel[len(REDIS_BAR_CHANNEL_PREFIX) :].rsplit(":", 1)[0]
        close_price = payload.get("close")
        timestamp = payload.get("timestamp")
        if close_price is None or timestamp is None:
            return

        ts = int(datetime.fromisoformat(str(timestamp).replace("Z", "+00:00")).timestamp())
        if ts < self._latest_price_ts.get(symbol, 0):
            return
        self._latest_price_ts[symbol] = ts
        self._latest_prices[symbol] = Decimal(str(close_price))

    async def _subscribe_to_symbol(self, symbol: str) -> None:
        """
        Subscribe to price updates for a symbol.

        Subscribes both published feed timeframes; whichever the instrument actually
        ingests at supplies the price, and `_handle_price_update` keeps the freshest.

        """
        if symbol in self._subscribed_symbols:
            return

        if self._pubsub:
            await self._pubsub.subscribe(bar_channel(symbol, "1m"), bar_channel(symbol, "5m"))
            self._subscribed_symbols.add(symbol)

    def _get_latest_price(self, instrument_id: InstrumentId) -> Price | None:
        """
        Get the latest price for an instrument.
        """
        symbol = instrument_id.symbol.value
        price_decimal = self._latest_prices.get(symbol)
        if price_decimal is not None:
            return Price.from_str(str(price_decimal))

        # Try to get from cache
        bar = self._cache.bar(instrument_id)
        if bar is not None:
            return bar.close

        return None

    # -- Order submission ----

    async def _submit_order(self, command: SubmitOrder) -> None:
        """
        Submit an order for simulated execution.
        """
        order = command.order
        symbol = order.instrument_id.symbol.value

        # Ensure we're subscribed to price updates for this symbol
        await self._subscribe_to_symbol(symbol)

        # Get current market price
        market_price = self._get_latest_price(order.instrument_id)

        if market_price is None:
            # No price available - reject the order
            self._log.warning(f"No price available for {symbol}, rejecting order")
            self.generate_order_rejected(
                strategy_id=order.strategy_id,
                instrument_id=order.instrument_id,
                client_order_id=order.client_order_id,
                reason=f"No market price available for {symbol}",
                ts_event=self._clock.timestamp_ns(),
            )
            return

        # Generate a venue order ID
        venue_order_id = VenueOrderId(f"BF-{UUID4().value[:12]}")

        # Generate order accepted event immediately
        self.generate_order_accepted(
            strategy_id=order.strategy_id,
            instrument_id=order.instrument_id,
            client_order_id=order.client_order_id,
            venue_order_id=venue_order_id,
            ts_event=self._clock.timestamp_ns(),
        )

        self._log.info(f"Order accepted: {order.client_order_id} -> {venue_order_id}")

        # For market orders, simulate fill after latency
        if order.order_type == OrderType.MARKET:
            # Simulate fill with the fill model
            fill_result = self._fill_model.simulate_fill(
                order_side=order.side,
                quantity=order.quantity,
                market_price=market_price,
            )

            # Schedule the fill after simulated latency
            await asyncio.sleep(fill_result.latency_ms / 1000.0)

            # Generate fill event
            self.generate_order_filled(
                strategy_id=order.strategy_id,
                instrument_id=order.instrument_id,
                client_order_id=order.client_order_id,
                venue_order_id=venue_order_id,
                venue_position_id=None,
                trade_id=TradeId(f"BF-{UUID4().value[:12]}"),
                order_side=order.side,
                order_type=order.order_type,
                last_qty=fill_result.fill_qty,
                last_px=fill_result.fill_price,
                quote_currency=Currency.from_str("USD"),
                commission=Money(0, Currency.from_str("USD")),
                liquidity_side=LiquiditySide.TAKER,
                ts_event=self._clock.timestamp_ns(),
            )

            self._log.info(
                f"Order filled: {order.client_order_id} "
                f"@ {fill_result.fill_price} (qty: {fill_result.fill_qty})",
            )

            # Handle partial fill if applicable
            if fill_result.is_partial:
                remaining_qty = Quantity.from_str(
                    str(Decimal(str(order.quantity)) - Decimal(str(fill_result.fill_qty))),
                )
                self._log.info(f"Partial fill, remaining qty: {remaining_qty}")
                # For simplicity, we'll fill the rest immediately
                # In a more realistic model, this could be delayed or left open

        else:
            # For limit/stop orders, store as pending (future enhancement)
            self._pending_orders[order.client_order_id.value] = command
            self._log.info(f"Limit/stop order queued: {order.client_order_id}")

    async def _cancel_order(self, command: CancelOrder) -> None:
        """
        Cancel a pending order.
        """
        client_order_id = command.client_order_id.value

        if client_order_id in self._pending_orders:
            del self._pending_orders[client_order_id]

            self.generate_order_canceled(
                strategy_id=command.strategy_id,
                instrument_id=command.instrument_id,
                client_order_id=command.client_order_id,
                venue_order_id=command.venue_order_id,
                ts_event=self._clock.timestamp_ns(),
            )

            self._log.info(f"Order canceled: {command.client_order_id}")
        else:
            self._log.warning(f"Order not found for cancel: {command.client_order_id}")
            self.generate_order_cancel_rejected(
                strategy_id=command.strategy_id,
                instrument_id=command.instrument_id,
                client_order_id=command.client_order_id,
                venue_order_id=command.venue_order_id,
                reason="Order not found",
                ts_event=self._clock.timestamp_ns(),
            )

    async def _modify_order(self, command: ModifyOrder) -> None:
        """
        Modify a pending order.
        """
        # For simplicity, reject all modify requests
        self._log.warning(f"Order modify not supported: {command.client_order_id}")
        self.generate_order_modify_rejected(
            strategy_id=command.strategy_id,
            instrument_id=command.instrument_id,
            client_order_id=command.client_order_id,
            venue_order_id=command.venue_order_id,
            reason="Order modification not supported in paper trading",
            ts_event=self._clock.timestamp_ns(),
        )

    # -- Reports ----

    async def generate_order_status_reports(
        self,
        command: GenerateOrderStatusReports,
    ) -> list[OrderStatusReport]:
        """
        Generate order status reports.
        """
        # For paper trading, we don't persist orders externally
        # Return empty list - the cache has the order state
        return []

    async def generate_fill_reports(
        self,
        command: GenerateFillReports,
    ) -> list[FillReport]:
        """
        Generate fill reports.
        """
        # For paper trading, fills are not persisted externally
        return []

    async def generate_position_status_reports(
        self,
        command: GeneratePositionStatusReports,
    ) -> list[PositionStatusReport]:
        """
        Generate position status reports.
        """
        # For paper trading, positions are tracked in the cache
        return []
