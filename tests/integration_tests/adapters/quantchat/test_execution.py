# -------------------------------------------------------------------------------------------------
#  QuantChat Local Paper Trading Adapter for Nautilus Trader
#  https://github.com/mandeltechnologies/quantchat.com
# -------------------------------------------------------------------------------------------------

import asyncio

import pytest

from nautilus_trader.model.currencies import USD
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.events import OrderCanceled
from nautilus_trader.model.events import OrderFilled


def seed_price(exec_client, symbol: str, price: str) -> None:
    """
    Seed the client's latest-price map directly (stands in for the Redis feed).
    """
    from decimal import Decimal

    exec_client._latest_prices[symbol] = Decimal(price)
    exec_client._latest_price_ts[symbol] = 2_000_000_000


async def _settle(seconds: float = 0.2) -> None:
    # Let the scheduled fill task (1ms latency) and engine processing run.
    await asyncio.sleep(seconds)


def _free_usd(portfolio, venue) -> float:
    account = portfolio.account(venue)
    assert account is not None
    money = account.balances_free().get(USD)
    return float(money.as_double()) if money is not None else 0.0


@pytest.mark.asyncio
async def test_execution_client_uses_multi_currency_cash_account(exec_client):
    assert exec_client.base_currency is None


@pytest.mark.asyncio
async def test_fill_depletes_free_balance(
    exec_client,
    instrument,
    strategy,
    portfolio,
    venue,
    events,
):
    # Arrange: $100k cash, price $100.
    await exec_client._connect()
    exec_client._set_connected(True)
    seed_price(exec_client, "AAPL", "100")
    assert _free_usd(portfolio, venue) == pytest.approx(100_000.0)

    # Act: spend half the cash.
    order = strategy.order_factory.market(
        instrument_id=instrument.id,
        order_side=OrderSide.BUY,
        quantity=instrument.make_qty(500),
    )
    strategy.submit_order(order)
    await _settle()

    # The fill itself must have happened for the balance assertion to mean
    # anything.
    assert any(isinstance(e, OrderFilled) for e in events)

    # Assert: the venue reports the depleted balance — sizing the next order
    # against balances_free() must see $50k, not the seeded $100k.
    assert _free_usd(portfolio, venue) == pytest.approx(50_000.0)


@pytest.mark.asyncio
async def test_full_cash_rebuy_cannot_overdraw(
    exec_client,
    instrument,
    strategy,
    portfolio,
    venue,
    events,
):
    # Arrange: $100k cash, price $100; buy the full balance.
    await exec_client._connect()
    exec_client._set_connected(True)
    seed_price(exec_client, "AAPL", "100")

    first = strategy.order_factory.market(
        instrument_id=instrument.id,
        order_side=OrderSide.BUY,
        quantity=instrument.make_qty(1_000),
    )
    strategy.submit_order(first)
    await _settle()
    assert _free_usd(portfolio, venue) == pytest.approx(0.0)

    # Act: a second "buy 100% of cash" sized against the stale pre-fill
    # balance — the exact prod failure that drove backend cash negative.
    second = strategy.order_factory.market(
        instrument_id=instrument.id,
        order_side=OrderSide.BUY,
        quantity=instrument.make_qty(1_000),
    )
    strategy.submit_order(second)
    await _settle()

    # Assert: no fill happens (canceled for insufficient cash) and the venue
    # balance never goes negative.
    fills = [e for e in events if isinstance(e, OrderFilled)]
    cancels = [e for e in events if isinstance(e, OrderCanceled)]
    assert len(fills) == 1
    assert any(c.client_order_id == second.client_order_id for c in cancels)
    assert _free_usd(portfolio, venue) >= 0.0


@pytest.mark.asyncio
async def test_sell_restores_free_balance(
    exec_client,
    instrument,
    strategy,
    portfolio,
    venue,
    events,
):
    # Arrange: buy 500 @ $100, then sell it back at $120.
    await exec_client._connect()
    exec_client._set_connected(True)
    seed_price(exec_client, "AAPL", "100")

    buy = strategy.order_factory.market(
        instrument_id=instrument.id,
        order_side=OrderSide.BUY,
        quantity=instrument.make_qty(500),
    )
    strategy.submit_order(buy)
    await _settle()

    seed_price(exec_client, "AAPL", "120")
    sell = strategy.order_factory.market(
        instrument_id=instrument.id,
        order_side=OrderSide.SELL,
        quantity=instrument.make_qty(500),
    )
    strategy.submit_order(sell)
    await _settle()

    # Assert: 100k - 50k + 60k.
    assert _free_usd(portfolio, venue) == pytest.approx(110_000.0)
