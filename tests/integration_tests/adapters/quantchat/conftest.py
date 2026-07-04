# -------------------------------------------------------------------------------------------------
#  QuantChat Local Paper Trading Adapter for Nautilus Trader
#  https://github.com/mandeltechnologies/quantchat.com
# -------------------------------------------------------------------------------------------------

from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest

from nautilus_trader.adapters.quantchat.config import QuantChatExecClientConfig
from nautilus_trader.adapters.quantchat.constants import QUANTCHAT_PAPER_ACCOUNT_ID
from nautilus_trader.adapters.quantchat.constants import QUANTCHAT_VENUE
from nautilus_trader.adapters.quantchat.execution import QuantChatExecutionClient
from nautilus_trader.adapters.quantchat.providers import QuantChatInstrumentProvider
from nautilus_trader.config import InstrumentProviderConfig
from nautilus_trader.model.events import AccountState
from nautilus_trader.model.identifiers import Venue
from nautilus_trader.test_kit.providers import TestInstrumentProvider
from nautilus_trader.test_kit.stubs.events import TestEventStubs


@pytest.fixture
def venue() -> Venue:
    return QUANTCHAT_VENUE


@pytest.fixture
def instrument():
    return TestInstrumentProvider.equity("AAPL", QUANTCHAT_VENUE.value)


@pytest.fixture
def instrument_provider(clock, instrument):
    provider = QuantChatInstrumentProvider(clock=clock, config=InstrumentProviderConfig())
    provider.add(instrument)
    return provider


@pytest.fixture
def fake_pubsub(monkeypatch):
    """
    Replace the Redis price feed with a no-op so tests run hermetically; prices are
    seeded directly on the client.
    """
    fake = MagicMock()
    fake.start = AsyncMock()
    fake.stop = AsyncMock()
    fake.subscribe = AsyncMock()
    monkeypatch.setattr(
        "nautilus_trader.adapters.quantchat.execution.ResilientPubSub",
        MagicMock(return_value=fake),
    )
    return fake


@pytest.fixture
def exec_client(
    fake_pubsub,
    instrument,
    instrument_provider,
    event_loop,
    msgbus,
    cache,
    clock,
):
    config = QuantChatExecClientConfig(
        starting_balance="100000 USD",
        base_latency_ms=1,
        slippage_bps=0.0,
    )
    return QuantChatExecutionClient(
        loop=event_loop,
        msgbus=msgbus,
        cache=cache,
        clock=clock,
        instrument_provider=instrument_provider,
        config=config,
        name=None,
    )


@pytest.fixture
def data_client():
    return None


@pytest.fixture
def account_state() -> AccountState:
    # Multi-currency (base_currency=None), matching the client's account shape.
    return TestEventStubs.cash_account_state(
        account_id=QUANTCHAT_PAPER_ACCOUNT_ID,
        base_currency=None,
    )
