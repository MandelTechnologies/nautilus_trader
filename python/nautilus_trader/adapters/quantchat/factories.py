# -------------------------------------------------------------------------------------------------
#  QuantChat Local Paper Trading Adapter for Nautilus Trader
#  https://github.com/mandeltechnologies/quantchat
# -------------------------------------------------------------------------------------------------

from __future__ import annotations

import asyncio
from functools import lru_cache

from nautilus_trader.adapters.quantchat.config import QuantChatDataClientConfig
from nautilus_trader.adapters.quantchat.config import QuantChatExecClientConfig
from nautilus_trader.adapters.quantchat.data import QuantChatDataClient
from nautilus_trader.adapters.quantchat.execution import QuantChatExecutionClient
from nautilus_trader.adapters.quantchat.providers import QuantChatInstrumentProvider
from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.component import MessageBus
from nautilus_trader.config import InstrumentProviderConfig
from nautilus_trader.live.factories import LiveDataClientFactory
from nautilus_trader.live.factories import LiveExecClientFactory


@lru_cache(1)
def get_cached_quantchat_instrument_provider(
    clock: LiveClock,
    config: InstrumentProviderConfig,
) -> QuantChatInstrumentProvider:
    """
    Cache and return a Botfolio instrument provider.

    If a cached provider already exists, then that provider will be returned.

    Parameters
    ----------
    clock : LiveClock
        The clock for the provider.
    config : InstrumentProviderConfig
        The configuration for the provider.

    Returns
    -------
    QuantChatInstrumentProvider

    """
    return QuantChatInstrumentProvider(
        clock=clock,
        config=config,
    )


class QuantChatLiveDataClientFactory(LiveDataClientFactory):
    """
    Provides a Botfolio live data client factory.
    """

    @staticmethod
    def create(  # type: ignore
        loop: asyncio.AbstractEventLoop,
        name: str,
        config: QuantChatDataClientConfig,
        msgbus: MessageBus,
        cache: Cache,
        clock: LiveClock,
    ) -> QuantChatDataClient:
        """
        Create a new Botfolio data client.

        Parameters
        ----------
        loop : asyncio.AbstractEventLoop
            The event loop for the client.
        name : str
            The custom client ID.
        config : QuantChatDataClientConfig
            The client configuration.
        msgbus : MessageBus
            The message bus for the client.
        cache : Cache
            The cache for the client.
        clock : LiveClock
            The clock for the client.

        Returns
        -------
        QuantChatDataClient

        """
        # Get instrument provider singleton
        provider = get_cached_quantchat_instrument_provider(
            clock=clock,
            config=config.instrument_provider,
        )

        return QuantChatDataClient(
            loop=loop,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            instrument_provider=provider,
            config=config,
            name=name,
        )


class QuantChatLiveExecClientFactory(LiveExecClientFactory):
    """
    Provides a Botfolio live execution client factory.
    """

    @staticmethod
    def create(  # type: ignore
        loop: asyncio.AbstractEventLoop,
        name: str,
        config: QuantChatExecClientConfig,
        msgbus: MessageBus,
        cache: Cache,
        clock: LiveClock,
    ) -> QuantChatExecutionClient:
        """
        Create a new Botfolio execution client.

        Parameters
        ----------
        loop : asyncio.AbstractEventLoop
            The event loop for the client.
        name : str
            The custom client ID.
        config : QuantChatExecClientConfig
            The client configuration.
        msgbus : MessageBus
            The message bus for the client.
        cache : Cache
            The cache for the client.
        clock : LiveClock
            The clock for the client.

        Returns
        -------
        QuantChatExecutionClient

        """
        # Get instrument provider singleton
        provider = get_cached_quantchat_instrument_provider(
            clock=clock,
            config=config.instrument_provider,
        )

        return QuantChatExecutionClient(
            loop=loop,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            instrument_provider=provider,
            config=config,
            name=name,
        )
