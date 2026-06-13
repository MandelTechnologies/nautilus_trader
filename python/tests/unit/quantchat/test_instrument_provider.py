# -------------------------------------------------------------------------------------------------
#  Regression tests for QuantChatInstrumentProvider symbol-based instrument creation.
#  The equity path was never exercised by a backtest until the first SPY run in prod
#  crashed the container: _create_equity passed max_price/min_price kwargs that this
#  fork's Equity.__init__ does not accept.
# -------------------------------------------------------------------------------------------------

from decimal import Decimal

import pytest


pytest.importorskip("nautilus_trader.backtest.engine", reason="requires built nautilus core")

from nautilus_trader.adapters.quantchat.providers import QuantChatInstrumentProvider
from nautilus_trader.common.component import LiveClock
from nautilus_trader.config import InstrumentProviderConfig
from nautilus_trader.model.instruments import CurrencyPair
from nautilus_trader.model.instruments import Equity


def _provider() -> QuantChatInstrumentProvider:
    return QuantChatInstrumentProvider(
        clock=LiveClock(),
        config=InstrumentProviderConfig(load_all=False),
    )


def test_create_equity_instrument():
    fee = Decimal("0.0005")

    instrument = _provider()._create_instrument("SPY", maker_fee=fee, taker_fee=fee)

    assert isinstance(instrument, Equity)
    assert str(instrument.id.symbol) == "SPY"
    assert instrument.price_precision == 2
    assert instrument.taker_fee == fee


def test_create_currency_pair_instrument():
    instrument = _provider()._create_instrument("BTC/USD")

    assert isinstance(instrument, CurrencyPair)
    assert str(instrument.id.symbol) == "BTC/USD"
