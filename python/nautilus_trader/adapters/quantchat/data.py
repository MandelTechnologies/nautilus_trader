# -------------------------------------------------------------------------------------------------
#  QuantChat Local Paper Trading Adapter for Nautilus Trader
#  https://github.com/mandeltechnologies/quantchat.com
# -------------------------------------------------------------------------------------------------

from __future__ import annotations

import asyncio
from datetime import datetime
import json
from typing import Any

from nautilus_trader.adapters.quantchat.config import QuantChatDataClientConfig
from nautilus_trader.adapters.quantchat.constants import QUANTCHAT_VENUE
from nautilus_trader.adapters.quantchat.constants import REDIS_QUOTE_CHANNEL_PREFIX
from nautilus_trader.adapters.quantchat.constants import bar_channel
from nautilus_trader.adapters.quantchat.constants import bar_spec_timeframe
from nautilus_trader.adapters.quantchat.providers import QuantChatInstrumentProvider
from nautilus_trader.adapters.quantchat.pubsub import ResilientPubSub
from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.component import MessageBus
from nautilus_trader.common.enums import LogColor
from nautilus_trader.core.datetime import dt_to_unix_nanos
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.data.messages import SubscribeBars
from nautilus_trader.data.messages import SubscribeQuoteTicks
from nautilus_trader.data.messages import SubscribeTradeTicks
from nautilus_trader.data.messages import UnsubscribeBars
from nautilus_trader.data.messages import UnsubscribeQuoteTicks
from nautilus_trader.data.messages import UnsubscribeTradeTicks
from nautilus_trader.live.data_client import LiveMarketDataClient
from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import BarType
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.data import TradeTick
from nautilus_trader.model.enums import AggressorSide
from nautilus_trader.model.identifiers import ClientId
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import Symbol
from nautilus_trader.model.identifiers import TradeId
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity


class QuantChatDataClient(LiveMarketDataClient):
    """
    Provides a data client for QuantChat local paper trading.

    Receives finalized bars over Redis pub/sub channels published by the quantchat
    backend market-data writer (`market:bar:{symbol}:{timeframe}`).

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
    config : QuantChatDataClientConfig
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
        config: QuantChatDataClientConfig,
        name: str | None = None,
    ) -> None:
        super().__init__(
            loop=loop,
            client_id=ClientId(name or "QUANTCHAT"),
            venue=QUANTCHAT_VENUE,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            instrument_provider=instrument_provider,
            config=config,
        )

        self._config = config
        self._redis_url = config.redis_url
        self._can_access_tick_data = config.can_access_tick_data

        self._pubsub: ResilientPubSub | None = None

        # Bar subscriptions keyed by channel, plus the last delivered bar timestamp per
        # channel so duplicate or out-of-order publishes never reach the strategy.
        self._bar_types: dict[str, BarType] = {}
        self._last_bar_ts: dict[str, int] = {}
        self._subscribed_quote_symbols: set[str] = set()

    async def _connect(self) -> None:
        self._pubsub = ResilientPubSub(self._redis_url, self._on_message, self._log)
        await self._pubsub.start()
        self._log.info("QuantChat data client connected", LogColor.GREEN)

    async def _disconnect(self) -> None:
        if self._pubsub:
            await self._pubsub.stop()
            self._pubsub = None

        self._bar_types.clear()
        self._last_bar_ts.clear()
        self._subscribed_quote_symbols.clear()

        self._log.info("QuantChat data client disconnected")

    def _on_message(self, channel: str, data: str) -> None:
        payload = json.loads(data)
        if channel in self._bar_types:
            self._handle_bar_message(channel, payload)
        elif channel.startswith(REDIS_QUOTE_CHANNEL_PREFIX):
            symbol = channel[len(REDIS_QUOTE_CHANNEL_PREFIX) :]
            self._handle_quote_message(symbol, payload)

    def _handle_bar_message(self, channel: str, data: dict[str, Any]) -> None:
        bar_type = self._bar_types.get(channel)
        if bar_type is None:
            return

        try:
            ts_event = _parse_bar_timestamp(data["timestamp"])
            bar = Bar(
                bar_type=bar_type,
                open=Price.from_str(str(data["open"])),
                high=Price.from_str(str(data["high"])),
                low=Price.from_str(str(data["low"])),
                close=Price.from_str(str(data["close"])),
                volume=Quantity.from_str(str(data["volume"])),
                ts_event=ts_event,
                ts_init=self._clock.timestamp_ns(),
            )
        except (KeyError, ValueError) as e:
            self._log.error(f"Dropping malformed bar payload on {channel}: {e}")
            return

        # The backend re-publishes recent buckets when fetch windows overlap; the strategy
        # must only ever see each bucket once, in order.
        if ts_event <= self._last_bar_ts.get(channel, 0):
            return
        self._last_bar_ts[channel] = ts_event

        self._handle_data(bar)

    def _handle_quote_message(self, symbol: str, data: dict[str, Any]) -> None:
        instrument_id = InstrumentId(
            symbol=Symbol(symbol),
            venue=QUANTCHAT_VENUE,
        )

        price = data.get("price", 0)
        ts_event = self._parse_timestamp_ms(data.get("timestamp", 0))

        # Create a quote tick with bid/ask spread around the price
        # For simplicity, use the same price for bid/ask (no spread)
        quote = QuoteTick(
            instrument_id=instrument_id,
            bid_price=Price.from_str(str(price)),
            ask_price=Price.from_str(str(price)),
            bid_size=Quantity.from_str(str(data.get("volume", 1))),
            ask_size=Quantity.from_str(str(data.get("volume", 1))),
            ts_event=ts_event,
            ts_init=self._clock.timestamp_ns(),
        )

        self._handle_data(quote)

        # Also create a trade tick from the quote
        trade = TradeTick(
            instrument_id=instrument_id,
            price=Price.from_str(str(price)),
            size=Quantity.from_str(str(data.get("volume", 1))),
            aggressor_side=AggressorSide.NO_AGGRESSOR,
            trade_id=TradeId(str(UUID4())),
            ts_event=ts_event,
            ts_init=self._clock.timestamp_ns(),
        )

        self._handle_data(trade)

    def _parse_timestamp_ms(self, ts_ms: float) -> int:
        if not ts_ms:
            return self._clock.timestamp_ns()
        return int(ts_ms * 1_000_000)  # ms to ns

    # -- Subscriptions ----

    async def _subscribe_quote_ticks(self, command: SubscribeQuoteTicks) -> None:
        """
        Subscribe to quote ticks for an instrument.

        Requires PRO or ELITE membership tier. HOBBYIST users will receive a warning and
        the subscription will be skipped.

        """
        if not self._can_access_tick_data:
            self._log.warning(
                "Quote tick subscription requires PRO or ELITE membership. "
                "Upgrade your plan to access tick data.",
            )
            return

        symbol = command.instrument_id.symbol.value
        if symbol in self._subscribed_quote_symbols or not self._pubsub:
            return

        await self._pubsub.subscribe(f"{REDIS_QUOTE_CHANNEL_PREFIX}{symbol}")
        self._subscribed_quote_symbols.add(symbol)

    async def _subscribe_trade_ticks(self, command: SubscribeTradeTicks) -> None:
        """
        Subscribe to trade ticks for an instrument.

        Requires PRO or ELITE membership tier. HOBBYIST users will receive a warning and
        the subscription will be skipped.

        """
        if not self._can_access_tick_data:
            self._log.warning(
                "Trade tick subscription requires PRO or ELITE membership. "
                "Upgrade your plan to access tick data.",
            )
            return

        # Trade ticks come from the same quote channel
        symbol = command.instrument_id.symbol.value
        if symbol in self._subscribed_quote_symbols or not self._pubsub:
            return

        await self._pubsub.subscribe(f"{REDIS_QUOTE_CHANNEL_PREFIX}{symbol}")
        self._subscribed_quote_symbols.add(symbol)

    async def _subscribe_bars(self, command: SubscribeBars) -> None:
        bar_type = command.bar_type
        timeframe = bar_spec_timeframe(bar_type.spec)
        if timeframe is None:
            self._log.error(f"Unsupported bar specification: {bar_type}")
            return

        channel = bar_channel(bar_type.instrument_id.symbol.value, timeframe)
        if channel in self._bar_types or not self._pubsub:
            return

        self._bar_types[channel] = bar_type
        await self._pubsub.subscribe(channel)

    async def _unsubscribe_quote_ticks(self, command: UnsubscribeQuoteTicks) -> None:
        symbol = command.instrument_id.symbol.value
        if symbol not in self._subscribed_quote_symbols or not self._pubsub:
            return

        await self._pubsub.unsubscribe(f"{REDIS_QUOTE_CHANNEL_PREFIX}{symbol}")
        self._subscribed_quote_symbols.discard(symbol)

    async def _unsubscribe_trade_ticks(self, command: UnsubscribeTradeTicks) -> None:
        symbol = command.instrument_id.symbol.value
        if symbol not in self._subscribed_quote_symbols or not self._pubsub:
            return

        await self._pubsub.unsubscribe(f"{REDIS_QUOTE_CHANNEL_PREFIX}{symbol}")
        self._subscribed_quote_symbols.discard(symbol)

    async def _unsubscribe_bars(self, command: UnsubscribeBars) -> None:
        timeframe = bar_spec_timeframe(command.bar_type.spec)
        if timeframe is None:
            return

        channel = bar_channel(command.bar_type.instrument_id.symbol.value, timeframe)
        if channel not in self._bar_types or not self._pubsub:
            return

        await self._pubsub.unsubscribe(channel)
        self._bar_types.pop(channel, None)
        self._last_bar_ts.pop(channel, None)


def _parse_bar_timestamp(value: str) -> int:
    """
    Parse an ISO-8601 bar timestamp to UNIX nanoseconds.

    Raises ``ValueError`` for malformed input; bar payloads without a valid timestamp
    must be dropped rather than stamped with the local clock.

    """
    return dt_to_unix_nanos(datetime.fromisoformat(str(value).replace("Z", "+00:00")))
