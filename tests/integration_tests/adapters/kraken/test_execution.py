# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2015-2025 Nautech Systems Pty Ltd. All rights reserved.
#  https://nautechsystems.io
#
#  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
# -------------------------------------------------------------------------------------------------

from decimal import Decimal
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest

from nautilus_trader.adapters.kraken.config import KrakenExecClientConfig
from nautilus_trader.adapters.kraken.constants import KRAKEN_VENUE
from nautilus_trader.adapters.kraken.execution import KrakenExecutionClient
from nautilus_trader.core import nautilus_pyo3
from nautilus_trader.execution.messages import CancelAllOrders
from nautilus_trader.execution.messages import CancelOrder
from nautilus_trader.execution.messages import GenerateFillReports
from nautilus_trader.execution.messages import GenerateOrderStatusReports
from nautilus_trader.execution.messages import GeneratePositionStatusReports
from nautilus_trader.execution.messages import SubmitOrder
from nautilus_trader.model.currencies import BTC
from nautilus_trader.model.currencies import USD
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import TimeInForce
from nautilus_trader.model.identifiers import ClientOrderId
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import Symbol
from nautilus_trader.model.identifiers import VenueOrderId
from nautilus_trader.model.instruments import CryptoPerpetual
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity
from nautilus_trader.model.orders import LimitOrder
from nautilus_trader.model.orders import MarketOrder
from nautilus_trader.test_kit.stubs.identifiers import TestIdStubs
from tests.integration_tests.adapters.kraken.conftest import _create_exec_ws_mock_futures
from tests.integration_tests.adapters.kraken.conftest import _create_exec_ws_mock_spot


@pytest.fixture
def exec_client_builder_spot(
    event_loop,
    mock_http_client_spot,
    msgbus,
    cache,
    live_clock,
    mock_instrument_provider,
):
    """
    Build a KrakenExecutionClient configured for SPOT only.
    """

    def builder(monkeypatch, *, config_kwargs: dict | None = None):
        ws_spot_client = _create_exec_ws_mock_spot()

        monkeypatch.setattr(
            "nautilus_trader.adapters.kraken.execution.nautilus_pyo3.KrakenSpotWebSocketClient",
            lambda *args, **kwargs: ws_spot_client,
        )

        # Skip account registration wait in tests
        monkeypatch.setattr(
            "nautilus_trader.adapters.kraken.execution.KrakenExecutionClient._await_account_registered",
            AsyncMock(),
        )

        mock_http_client_spot.reset_mock()
        mock_instrument_provider.initialize.reset_mock()
        mock_instrument_provider.instruments_pyo3.reset_mock()
        mock_instrument_provider.instruments_pyo3.return_value = []

        config = KrakenExecClientConfig(
            api_key="test_api_key",
            api_secret="test_api_secret",
            product_types=(nautilus_pyo3.KrakenProductType.SPOT,),
            **(config_kwargs or {}),
        )

        client = KrakenExecutionClient(
            loop=event_loop,
            http_client_spot=mock_http_client_spot,
            http_client_futures=None,
            msgbus=msgbus,
            cache=cache,
            clock=live_clock,
            instrument_provider=mock_instrument_provider,
            config=config,
            name=None,
        )

        return client, ws_spot_client, mock_http_client_spot, mock_instrument_provider

    return builder


@pytest.fixture
def exec_client_builder_futures(
    event_loop,
    mock_http_client_futures,
    msgbus,
    cache,
    live_clock,
    mock_instrument_provider,
):
    """
    Build a KrakenExecutionClient configured for FUTURES only.
    """

    def builder(monkeypatch, *, config_kwargs: dict | None = None):
        ws_futures_client = _create_exec_ws_mock_futures()

        monkeypatch.setattr(
            "nautilus_trader.adapters.kraken.execution.nautilus_pyo3.KrakenFuturesWebSocketClient",
            lambda *args, **kwargs: ws_futures_client,
        )

        # Skip account registration wait in tests
        monkeypatch.setattr(
            "nautilus_trader.adapters.kraken.execution.KrakenExecutionClient._await_account_registered",
            AsyncMock(),
        )

        mock_http_client_futures.reset_mock()
        mock_instrument_provider.initialize.reset_mock()
        mock_instrument_provider.instruments_pyo3.reset_mock()
        mock_instrument_provider.instruments_pyo3.return_value = []

        config = KrakenExecClientConfig(
            api_key="test_api_key",
            api_secret="test_api_secret",
            product_types=(nautilus_pyo3.KrakenProductType.FUTURES,),
            **(config_kwargs or {}),
        )

        client = KrakenExecutionClient(
            loop=event_loop,
            http_client_spot=None,
            http_client_futures=mock_http_client_futures,
            msgbus=msgbus,
            cache=cache,
            clock=live_clock,
            instrument_provider=mock_instrument_provider,
            config=config,
            name=None,
        )

        return client, ws_futures_client, mock_http_client_futures, mock_instrument_provider

    return builder


@pytest.fixture
def futures_instrument() -> CryptoPerpetual:
    """
    Create a Kraken futures perpetual instrument for testing.
    """
    return CryptoPerpetual(
        instrument_id=InstrumentId(Symbol("PF_XBTUSD"), KRAKEN_VENUE),
        raw_symbol=Symbol("PF_XBTUSD"),
        base_currency=BTC,
        quote_currency=USD,
        settlement_currency=USD,
        is_inverse=True,
        price_precision=1,
        size_precision=0,
        price_increment=Price.from_str("0.1"),
        size_increment=Quantity.from_str("1"),
        max_quantity=Quantity.from_str("1000000"),
        min_quantity=Quantity.from_str("1"),
        max_notional=None,
        min_notional=None,
        max_price=Price.from_str("1000000.0"),
        min_price=Price.from_str("0.1"),
        margin_init=Decimal("0.02"),
        margin_maint=Decimal("0.01"),
        maker_fee=Decimal("0.0002"),
        taker_fee=Decimal("0.0005"),
        ts_event=0,
        ts_init=0,
    )


# ============================================================================
# LIFECYCLE TESTS
# ============================================================================


@pytest.mark.asyncio
async def test_connect_success_spot(exec_client_builder_spot, monkeypatch):
    # Arrange
    client, ws_client, http_client, instrument_provider = exec_client_builder_spot(
        monkeypatch,
    )

    # Act
    await client._connect()

    try:
        # Assert
        instrument_provider.initialize.assert_awaited_once()
        http_client.request_account_state.assert_awaited_once()
        ws_client.connect.assert_awaited_once()
        ws_client.wait_until_active.assert_awaited_once_with(timeout_secs=10.0)
        ws_client.authenticate.assert_awaited_once()
        ws_client.subscribe_executions.assert_awaited_once()
    finally:
        await client._disconnect()


@pytest.mark.asyncio
async def test_connect_success_futures(exec_client_builder_futures, monkeypatch):
    # Arrange
    client, ws_client, http_client, instrument_provider = exec_client_builder_futures(
        monkeypatch,
    )

    # Act
    await client._connect()

    try:
        # Assert
        instrument_provider.initialize.assert_awaited_once()
        http_client.request_account_state.assert_awaited_once()
        ws_client.connect.assert_awaited_once()
        ws_client.wait_until_active.assert_awaited_once_with(timeout_secs=10.0)
        ws_client.authenticate.assert_awaited_once()
        ws_client.subscribe_executions.assert_awaited_once()
    finally:
        await client._disconnect()


@pytest.mark.asyncio
async def test_disconnect_success_spot(exec_client_builder_spot, monkeypatch):
    # Arrange
    client, ws_client, http_client, instrument_provider = exec_client_builder_spot(
        monkeypatch,
    )
    await client._connect()

    # Act
    await client._disconnect()

    # Assert
    http_client.cancel_all_requests.assert_called_once()
    ws_client.close.assert_awaited()


@pytest.mark.asyncio
async def test_disconnect_success_futures(exec_client_builder_futures, monkeypatch):
    # Arrange
    client, ws_client, http_client, instrument_provider = exec_client_builder_futures(
        monkeypatch,
    )
    await client._connect()

    # Act
    await client._disconnect()

    # Assert
    http_client.cancel_all_requests.assert_called_once()
    ws_client.close.assert_awaited()


@pytest.mark.asyncio
async def test_account_id_set_on_initialization_spot(exec_client_builder_spot, monkeypatch):
    # Arrange
    client, ws_client, http_client, instrument_provider = exec_client_builder_spot(
        monkeypatch,
    )

    # Assert
    assert client.account_id.value == "KRAKEN-UNIFIED"

    # Act
    await client._connect()

    try:
        # Assert - account_id remains UNIFIED after connection
        assert client.account_id.value == "KRAKEN-UNIFIED"
    finally:
        await client._disconnect()


# ============================================================================
# ORDER SUBMISSION TESTS
# ============================================================================


@pytest.mark.asyncio
async def test_submit_market_order_spot(exec_client_builder_spot, monkeypatch, instrument):
    # Arrange
    client, ws_client, http_client, instrument_provider = exec_client_builder_spot(
        monkeypatch,
    )

    await client._connect()

    order = MarketOrder(
        trader_id=TestIdStubs.trader_id(),
        strategy_id=TestIdStubs.strategy_id(),
        instrument_id=instrument.id,
        client_order_id=ClientOrderId("O-123456"),
        order_side=OrderSide.BUY,
        quantity=Quantity.from_str("0.100"),
        time_in_force=TimeInForce.IOC,
        reduce_only=False,
        quote_quantity=False,
        init_id=TestIdStubs.uuid(),
        ts_init=0,
    )

    command = SubmitOrder(
        trader_id=order.trader_id,
        strategy_id=order.strategy_id,
        order=order,
        command_id=TestIdStubs.uuid(),
        ts_init=0,
        position_id=None,
        client_id=None,
    )

    try:
        # Act
        await client._submit_order(command)

        # Assert - Kraken uses HTTP for order submission
        http_client.submit_order.assert_awaited_once()
    finally:
        await client._disconnect()


@pytest.mark.asyncio
async def test_submit_limit_order_spot(exec_client_builder_spot, monkeypatch, instrument):
    # Arrange
    client, ws_client, http_client, instrument_provider = exec_client_builder_spot(
        monkeypatch,
    )

    await client._connect()

    order = LimitOrder(
        trader_id=TestIdStubs.trader_id(),
        strategy_id=TestIdStubs.strategy_id(),
        instrument_id=instrument.id,
        client_order_id=ClientOrderId("O-123456"),
        order_side=OrderSide.BUY,
        quantity=Quantity.from_str("0.100"),
        price=Price.from_str("50000.0"),
        init_id=TestIdStubs.uuid(),
        ts_init=0,
    )

    command = SubmitOrder(
        trader_id=order.trader_id,
        strategy_id=order.strategy_id,
        order=order,
        command_id=TestIdStubs.uuid(),
        ts_init=0,
        position_id=None,
        client_id=None,
    )

    try:
        # Act
        await client._submit_order(command)

        # Assert - Kraken uses HTTP for order submission
        http_client.submit_order.assert_awaited_once()
    finally:
        await client._disconnect()


@pytest.mark.asyncio
async def test_submit_market_order_futures(
    exec_client_builder_futures,
    monkeypatch,
    futures_instrument,
    cache,
):
    # Arrange
    cache.add_instrument(futures_instrument)

    client, ws_client, http_client, instrument_provider = exec_client_builder_futures(
        monkeypatch,
    )

    await client._connect()

    order = MarketOrder(
        trader_id=TestIdStubs.trader_id(),
        strategy_id=TestIdStubs.strategy_id(),
        instrument_id=futures_instrument.id,
        client_order_id=ClientOrderId("O-123456"),
        order_side=OrderSide.BUY,
        quantity=Quantity.from_str("100"),
        time_in_force=TimeInForce.IOC,
        reduce_only=False,
        quote_quantity=False,
        init_id=TestIdStubs.uuid(),
        ts_init=0,
    )

    command = SubmitOrder(
        trader_id=order.trader_id,
        strategy_id=order.strategy_id,
        order=order,
        command_id=TestIdStubs.uuid(),
        ts_init=0,
        position_id=None,
        client_id=None,
    )

    try:
        # Act
        await client._submit_order(command)

        # Assert - Kraken uses HTTP for order submission
        http_client.submit_order.assert_awaited_once()
    finally:
        await client._disconnect()


@pytest.mark.asyncio
async def test_submit_limit_order_futures(
    exec_client_builder_futures,
    monkeypatch,
    futures_instrument,
    cache,
):
    # Arrange
    cache.add_instrument(futures_instrument)

    client, ws_client, http_client, instrument_provider = exec_client_builder_futures(
        monkeypatch,
    )

    await client._connect()

    order = LimitOrder(
        trader_id=TestIdStubs.trader_id(),
        strategy_id=TestIdStubs.strategy_id(),
        instrument_id=futures_instrument.id,
        client_order_id=ClientOrderId("O-123456"),
        order_side=OrderSide.BUY,
        quantity=Quantity.from_str("100"),
        price=Price.from_str("50000.0"),
        init_id=TestIdStubs.uuid(),
        ts_init=0,
    )

    command = SubmitOrder(
        trader_id=order.trader_id,
        strategy_id=order.strategy_id,
        order=order,
        command_id=TestIdStubs.uuid(),
        ts_init=0,
        position_id=None,
        client_id=None,
    )

    try:
        # Act
        await client._submit_order(command)

        # Assert - Kraken uses HTTP for order submission
        http_client.submit_order.assert_awaited_once()
    finally:
        await client._disconnect()


# ============================================================================
# ORDER CANCELLATION TESTS
# ============================================================================


@pytest.mark.asyncio
async def test_cancel_order_by_client_id_spot(
    exec_client_builder_spot,
    monkeypatch,
    instrument,
    cache,
):
    # Arrange
    client, ws_client, http_client, instrument_provider = exec_client_builder_spot(
        monkeypatch,
    )

    await client._connect()

    order = LimitOrder(
        trader_id=TestIdStubs.trader_id(),
        strategy_id=TestIdStubs.strategy_id(),
        instrument_id=instrument.id,
        client_order_id=ClientOrderId("O-123456"),
        order_side=OrderSide.BUY,
        quantity=Quantity.from_str("0.100"),
        price=Price.from_str("50000.0"),
        init_id=TestIdStubs.uuid(),
        ts_init=0,
    )

    cache.add_order(order, None)

    command = CancelOrder(
        trader_id=order.trader_id,
        strategy_id=order.strategy_id,
        instrument_id=order.instrument_id,
        client_order_id=order.client_order_id,
        venue_order_id=None,
        command_id=TestIdStubs.uuid(),
        ts_init=0,
        client_id=None,
    )

    try:
        # Act
        await client._cancel_order(command)

        # Assert - Kraken uses HTTP for order cancellation
        http_client.cancel_order.assert_awaited_once()
    finally:
        await client._disconnect()


@pytest.mark.asyncio
async def test_cancel_order_by_venue_id_spot(
    exec_client_builder_spot,
    monkeypatch,
    instrument,
    cache,
):
    # Arrange
    client, ws_client, http_client, instrument_provider = exec_client_builder_spot(
        monkeypatch,
    )

    await client._connect()

    order = LimitOrder(
        trader_id=TestIdStubs.trader_id(),
        strategy_id=TestIdStubs.strategy_id(),
        instrument_id=instrument.id,
        client_order_id=ClientOrderId("O-123456"),
        order_side=OrderSide.BUY,
        quantity=Quantity.from_str("0.100"),
        price=Price.from_str("50000.0"),
        init_id=TestIdStubs.uuid(),
        ts_init=0,
    )

    cache.add_order(order, None)

    command = CancelOrder(
        trader_id=order.trader_id,
        strategy_id=order.strategy_id,
        instrument_id=order.instrument_id,
        client_order_id=order.client_order_id,
        venue_order_id=VenueOrderId("KRAKEN-12345"),
        command_id=TestIdStubs.uuid(),
        ts_init=0,
        client_id=None,
    )

    try:
        # Act
        await client._cancel_order(command)

        # Assert - Kraken uses HTTP for order cancellation
        http_client.cancel_order.assert_awaited_once()
    finally:
        await client._disconnect()


@pytest.mark.asyncio
async def test_cancel_all_orders_spot(exec_client_builder_spot, monkeypatch, instrument):
    # Arrange
    client, ws_client, http_client, instrument_provider = exec_client_builder_spot(
        monkeypatch,
    )
    await client._connect()

    command = CancelAllOrders(
        trader_id=TestIdStubs.trader_id(),
        strategy_id=TestIdStubs.strategy_id(),
        instrument_id=instrument.id,
        order_side=OrderSide.NO_ORDER_SIDE,
        command_id=TestIdStubs.uuid(),
        ts_init=0,
        client_id=None,
    )

    http_client.cancel_all_orders.return_value = 0

    try:
        # Act
        await client._cancel_all_orders(command)

        # Assert
        http_client.cancel_all_orders.assert_awaited_once()
    finally:
        await client._disconnect()


@pytest.mark.asyncio
async def test_cancel_all_orders_futures(
    exec_client_builder_futures,
    monkeypatch,
    futures_instrument,
    cache,
):
    # Arrange
    cache.add_instrument(futures_instrument)

    client, ws_client, http_client, instrument_provider = exec_client_builder_futures(
        monkeypatch,
    )
    await client._connect()

    command = CancelAllOrders(
        trader_id=TestIdStubs.trader_id(),
        strategy_id=TestIdStubs.strategy_id(),
        instrument_id=futures_instrument.id,
        order_side=OrderSide.NO_ORDER_SIDE,
        command_id=TestIdStubs.uuid(),
        ts_init=0,
        client_id=None,
    )

    http_client.cancel_all_orders.return_value = 0

    try:
        # Act
        await client._cancel_all_orders(command)

        # Assert - Futures requires instrument_id parameter
        http_client.cancel_all_orders.assert_awaited_once()
    finally:
        await client._disconnect()


# ============================================================================
# ORDER REJECTION AND ERROR HANDLING TESTS
# ============================================================================


@pytest.mark.asyncio
async def test_submit_order_rejection_spot(exec_client_builder_spot, monkeypatch, instrument):
    # Arrange
    client, ws_client, http_client, instrument_provider = exec_client_builder_spot(
        monkeypatch,
    )
    await client._connect()

    order = MarketOrder(
        trader_id=TestIdStubs.trader_id(),
        strategy_id=TestIdStubs.strategy_id(),
        instrument_id=instrument.id,
        client_order_id=ClientOrderId("O-123456"),
        order_side=OrderSide.BUY,
        quantity=Quantity.from_str("0.100"),
        time_in_force=TimeInForce.IOC,
        reduce_only=False,
        quote_quantity=False,
        init_id=TestIdStubs.uuid(),
        ts_init=0,
    )

    command = SubmitOrder(
        trader_id=order.trader_id,
        strategy_id=order.strategy_id,
        order=order,
        command_id=TestIdStubs.uuid(),
        ts_init=0,
        position_id=None,
        client_id=None,
    )

    # Simulate order rejection
    http_client.submit_order.side_effect = Exception("Order rejected: Insufficient balance")

    try:
        # Act/Assert - Should not raise, but handle gracefully
        await client._submit_order(command)
    finally:
        await client._disconnect()


@pytest.mark.asyncio
async def test_cancel_order_rejection_spot(
    exec_client_builder_spot,
    monkeypatch,
    instrument,
    cache,
):
    # Arrange
    client, ws_client, http_client, instrument_provider = exec_client_builder_spot(
        monkeypatch,
    )
    await client._connect()

    order = LimitOrder(
        trader_id=TestIdStubs.trader_id(),
        strategy_id=TestIdStubs.strategy_id(),
        instrument_id=instrument.id,
        client_order_id=ClientOrderId("O-123456"),
        order_side=OrderSide.BUY,
        quantity=Quantity.from_str("0.100"),
        price=Price.from_str("50000.0"),
        init_id=TestIdStubs.uuid(),
        ts_init=0,
    )

    cache.add_order(order, None)

    command = CancelOrder(
        trader_id=order.trader_id,
        strategy_id=order.strategy_id,
        instrument_id=order.instrument_id,
        client_order_id=order.client_order_id,
        venue_order_id=VenueOrderId("KRAKEN-12345"),
        command_id=TestIdStubs.uuid(),
        ts_init=0,
        client_id=None,
    )

    http_client.cancel_order.side_effect = Exception("Order already filled")

    try:
        # Act/Assert - Should handle gracefully
        await client._cancel_order(command)
    finally:
        await client._disconnect()


# ============================================================================
# RECONCILIATION REPORT TESTS
# ============================================================================


@pytest.mark.asyncio
async def test_generate_order_status_reports_converts_results_spot(
    exec_client_builder_spot,
    monkeypatch,
    instrument,
):
    # Arrange
    client, _, http_client, _ = exec_client_builder_spot(monkeypatch)

    expected_report = MagicMock()
    monkeypatch.setattr(
        "nautilus_trader.adapters.kraken.execution.OrderStatusReport.from_pyo3",
        lambda obj: expected_report,
    )

    pyo3_report = MagicMock()
    http_client.request_order_status_reports.return_value = [pyo3_report]

    command = GenerateOrderStatusReports(
        instrument_id=instrument.id,
        start=None,
        end=None,
        open_only=True,
        command_id=TestIdStubs.uuid(),
        ts_init=0,
    )

    # Act
    reports = await client.generate_order_status_reports(command)

    # Assert
    http_client.request_order_status_reports.assert_awaited_once()
    assert reports == [expected_report]


@pytest.mark.asyncio
async def test_generate_order_status_reports_handles_failure_spot(
    exec_client_builder_spot,
    monkeypatch,
    instrument,
):
    # Arrange
    client, _, http_client, _ = exec_client_builder_spot(monkeypatch)
    http_client.request_order_status_reports.side_effect = Exception("boom")

    command = GenerateOrderStatusReports(
        instrument_id=instrument.id,
        start=None,
        end=None,
        open_only=False,
        command_id=TestIdStubs.uuid(),
        ts_init=0,
    )

    # Act
    reports = await client.generate_order_status_reports(command)

    # Assert
    assert reports == []


@pytest.mark.asyncio
async def test_generate_fill_reports_converts_results_spot(
    exec_client_builder_spot,
    monkeypatch,
    instrument,
):
    # Arrange
    client, _, http_client, _ = exec_client_builder_spot(monkeypatch)

    expected_report = MagicMock()
    monkeypatch.setattr(
        "nautilus_trader.adapters.kraken.execution.FillReport.from_pyo3",
        lambda obj: expected_report,
    )

    http_client.request_fill_reports.return_value = [MagicMock()]

    command = GenerateFillReports(
        instrument_id=instrument.id,
        venue_order_id=None,
        start=None,
        end=None,
        command_id=TestIdStubs.uuid(),
        ts_init=0,
    )

    # Act
    reports = await client.generate_fill_reports(command)

    # Assert
    http_client.request_fill_reports.assert_awaited_once()
    assert reports == [expected_report]


@pytest.mark.asyncio
async def test_generate_fill_reports_handles_failure_spot(
    exec_client_builder_spot,
    monkeypatch,
    instrument,
):
    # Arrange
    client, _, http_client, _ = exec_client_builder_spot(monkeypatch)
    http_client.request_fill_reports.side_effect = Exception("boom")

    command = GenerateFillReports(
        instrument_id=instrument.id,
        venue_order_id=None,
        start=None,
        end=None,
        command_id=TestIdStubs.uuid(),
        ts_init=0,
    )

    # Act
    reports = await client.generate_fill_reports(command)

    # Assert
    assert reports == []


@pytest.mark.asyncio
async def test_generate_position_status_reports_converts_results_spot(
    exec_client_builder_spot,
    monkeypatch,
):
    # Arrange
    client, _, http_client, _ = exec_client_builder_spot(monkeypatch)

    expected_report = MagicMock()
    monkeypatch.setattr(
        "nautilus_trader.adapters.kraken.execution.PositionStatusReport.from_pyo3",
        lambda obj: expected_report,
    )

    http_client.request_position_status_reports.return_value = [MagicMock()]

    command = GeneratePositionStatusReports(
        instrument_id=None,
        start=None,
        end=None,
        command_id=TestIdStubs.uuid(),
        ts_init=0,
    )

    # Act
    reports = await client.generate_position_status_reports(command)

    # Assert
    http_client.request_position_status_reports.assert_awaited_once()
    assert reports == [expected_report]


@pytest.mark.asyncio
async def test_generate_position_status_reports_handles_failure_spot(
    exec_client_builder_spot,
    monkeypatch,
):
    # Arrange
    client, _, http_client, _ = exec_client_builder_spot(monkeypatch)
    http_client.request_position_status_reports.side_effect = Exception("boom")

    command = GeneratePositionStatusReports(
        instrument_id=None,
        start=None,
        end=None,
        command_id=TestIdStubs.uuid(),
        ts_init=0,
    )

    # Act
    reports = await client.generate_position_status_reports(command)

    # Assert
    assert reports == []


@pytest.mark.asyncio
async def test_generate_order_status_reports_futures(
    exec_client_builder_futures,
    monkeypatch,
    futures_instrument,
    cache,
):
    # Arrange
    cache.add_instrument(futures_instrument)
    client, _, http_client, _ = exec_client_builder_futures(monkeypatch)

    expected_report = MagicMock()
    monkeypatch.setattr(
        "nautilus_trader.adapters.kraken.execution.OrderStatusReport.from_pyo3",
        lambda obj: expected_report,
    )

    pyo3_report = MagicMock()
    http_client.request_order_status_reports.return_value = [pyo3_report]

    command = GenerateOrderStatusReports(
        instrument_id=futures_instrument.id,
        start=None,
        end=None,
        open_only=True,
        command_id=TestIdStubs.uuid(),
        ts_init=0,
    )

    # Act
    reports = await client.generate_order_status_reports(command)

    # Assert
    http_client.request_order_status_reports.assert_awaited_once()
    assert reports == [expected_report]


@pytest.mark.asyncio
async def test_generate_fill_reports_futures(
    exec_client_builder_futures,
    monkeypatch,
    futures_instrument,
    cache,
):
    # Arrange
    cache.add_instrument(futures_instrument)
    client, _, http_client, _ = exec_client_builder_futures(monkeypatch)

    expected_report = MagicMock()
    monkeypatch.setattr(
        "nautilus_trader.adapters.kraken.execution.FillReport.from_pyo3",
        lambda obj: expected_report,
    )

    http_client.request_fill_reports.return_value = [MagicMock()]

    command = GenerateFillReports(
        instrument_id=futures_instrument.id,
        venue_order_id=None,
        start=None,
        end=None,
        command_id=TestIdStubs.uuid(),
        ts_init=0,
    )

    # Act
    reports = await client.generate_fill_reports(command)

    # Assert
    http_client.request_fill_reports.assert_awaited_once()
    assert reports == [expected_report]


# ============================================================================
# WEBSOCKET HANDLER TESTS
# ============================================================================


@pytest.mark.asyncio
async def test_handle_order_status_report_accepted(
    exec_client_builder_spot,
    monkeypatch,
    instrument,
    cache,
    msgbus,
):
    # Arrange
    client, ws_client, http_client, instrument_provider = exec_client_builder_spot(
        monkeypatch,
    )
    await client._connect()

    # Create and cache an order
    order = LimitOrder(
        trader_id=TestIdStubs.trader_id(),
        strategy_id=TestIdStubs.strategy_id(),
        instrument_id=instrument.id,
        client_order_id=ClientOrderId("O-123456"),
        order_side=OrderSide.BUY,
        quantity=Quantity.from_str("0.100"),
        price=Price.from_str("50000.0"),
        init_id=TestIdStubs.uuid(),
        ts_init=0,
    )
    cache.add_order(order, None)

    # Create pyo3 order status report
    pyo3_report = nautilus_pyo3.OrderStatusReport(
        account_id=nautilus_pyo3.AccountId("KRAKEN-UNIFIED"),
        instrument_id=nautilus_pyo3.InstrumentId.from_str(instrument.id.value),
        venue_order_id=nautilus_pyo3.VenueOrderId("KRAKEN-789"),
        client_order_id=nautilus_pyo3.ClientOrderId("O-123456"),
        order_side=nautilus_pyo3.OrderSide.BUY,
        order_type=nautilus_pyo3.OrderType.LIMIT,
        time_in_force=nautilus_pyo3.TimeInForce.GTC,
        order_status=nautilus_pyo3.OrderStatus.ACCEPTED,
        quantity=nautilus_pyo3.Quantity.from_str("0.100"),
        filled_qty=nautilus_pyo3.Quantity.from_str("0"),
        ts_accepted=0,
        ts_last=0,
        report_id=nautilus_pyo3.UUID4(),
        ts_init=0,
    )

    try:
        # Act
        client._handle_order_status_report_pyo3(pyo3_report)

        # Assert
        assert msgbus.sent_count > 0
    finally:
        await client._disconnect()


@pytest.mark.asyncio
async def test_handle_order_status_report_canceled(
    exec_client_builder_spot,
    monkeypatch,
    instrument,
    cache,
    msgbus,
):
    # Arrange
    client, ws_client, http_client, instrument_provider = exec_client_builder_spot(
        monkeypatch,
    )
    await client._connect()

    # Create and cache an order (must be in ACCEPTED state first)
    order = LimitOrder(
        trader_id=TestIdStubs.trader_id(),
        strategy_id=TestIdStubs.strategy_id(),
        instrument_id=instrument.id,
        client_order_id=ClientOrderId("O-123456"),
        order_side=OrderSide.BUY,
        quantity=Quantity.from_str("0.100"),
        price=Price.from_str("50000.0"),
        init_id=TestIdStubs.uuid(),
        ts_init=0,
    )
    cache.add_order(order, None)

    # Create pyo3 order status report
    pyo3_report = nautilus_pyo3.OrderStatusReport(
        account_id=nautilus_pyo3.AccountId("KRAKEN-UNIFIED"),
        instrument_id=nautilus_pyo3.InstrumentId.from_str(instrument.id.value),
        venue_order_id=nautilus_pyo3.VenueOrderId("KRAKEN-789"),
        client_order_id=nautilus_pyo3.ClientOrderId("O-123456"),
        order_side=nautilus_pyo3.OrderSide.BUY,
        order_type=nautilus_pyo3.OrderType.LIMIT,
        time_in_force=nautilus_pyo3.TimeInForce.GTC,
        order_status=nautilus_pyo3.OrderStatus.CANCELED,
        quantity=nautilus_pyo3.Quantity.from_str("0.100"),
        filled_qty=nautilus_pyo3.Quantity.from_str("0"),
        ts_accepted=0,
        ts_last=0,
        report_id=nautilus_pyo3.UUID4(),
        ts_init=0,
    )

    try:
        # Act
        client._handle_order_status_report_pyo3(pyo3_report)

        # Assert
        assert msgbus.sent_count > 0
    finally:
        await client._disconnect()


@pytest.mark.asyncio
async def test_handle_order_status_report_expired(
    exec_client_builder_spot,
    monkeypatch,
    instrument,
    cache,
    msgbus,
):
    # Arrange
    client, ws_client, http_client, instrument_provider = exec_client_builder_spot(
        monkeypatch,
    )
    await client._connect()

    order = LimitOrder(
        trader_id=TestIdStubs.trader_id(),
        strategy_id=TestIdStubs.strategy_id(),
        instrument_id=instrument.id,
        client_order_id=ClientOrderId("O-123456"),
        order_side=OrderSide.BUY,
        quantity=Quantity.from_str("0.100"),
        price=Price.from_str("50000.0"),
        init_id=TestIdStubs.uuid(),
        ts_init=0,
    )
    cache.add_order(order, None)

    pyo3_report = nautilus_pyo3.OrderStatusReport(
        account_id=nautilus_pyo3.AccountId("KRAKEN-UNIFIED"),
        instrument_id=nautilus_pyo3.InstrumentId.from_str(instrument.id.value),
        venue_order_id=nautilus_pyo3.VenueOrderId("KRAKEN-789"),
        client_order_id=nautilus_pyo3.ClientOrderId("O-123456"),
        order_side=nautilus_pyo3.OrderSide.BUY,
        order_type=nautilus_pyo3.OrderType.LIMIT,
        time_in_force=nautilus_pyo3.TimeInForce.GTD,
        order_status=nautilus_pyo3.OrderStatus.EXPIRED,
        quantity=nautilus_pyo3.Quantity.from_str("0.100"),
        filled_qty=nautilus_pyo3.Quantity.from_str("0"),
        ts_accepted=0,
        ts_last=0,
        report_id=nautilus_pyo3.UUID4(),
        ts_init=0,
    )

    try:
        # Act
        client._handle_order_status_report_pyo3(pyo3_report)

        # Assert
        assert msgbus.sent_count > 0
    finally:
        await client._disconnect()


@pytest.mark.asyncio
async def test_handle_order_status_report_rejected(
    exec_client_builder_spot,
    monkeypatch,
    instrument,
    cache,
    msgbus,
):
    # Arrange
    client, ws_client, http_client, instrument_provider = exec_client_builder_spot(
        monkeypatch,
    )
    await client._connect()

    order = LimitOrder(
        trader_id=TestIdStubs.trader_id(),
        strategy_id=TestIdStubs.strategy_id(),
        instrument_id=instrument.id,
        client_order_id=ClientOrderId("O-123456"),
        order_side=OrderSide.BUY,
        quantity=Quantity.from_str("0.100"),
        price=Price.from_str("50000.0"),
        init_id=TestIdStubs.uuid(),
        ts_init=0,
    )
    cache.add_order(order, None)

    pyo3_report = nautilus_pyo3.OrderStatusReport(
        account_id=nautilus_pyo3.AccountId("KRAKEN-UNIFIED"),
        instrument_id=nautilus_pyo3.InstrumentId.from_str(instrument.id.value),
        venue_order_id=nautilus_pyo3.VenueOrderId("KRAKEN-789"),
        client_order_id=nautilus_pyo3.ClientOrderId("O-123456"),
        order_side=nautilus_pyo3.OrderSide.BUY,
        order_type=nautilus_pyo3.OrderType.LIMIT,
        time_in_force=nautilus_pyo3.TimeInForce.GTC,
        order_status=nautilus_pyo3.OrderStatus.REJECTED,
        quantity=nautilus_pyo3.Quantity.from_str("0.100"),
        filled_qty=nautilus_pyo3.Quantity.from_str("0"),
        ts_accepted=0,
        ts_last=0,
        report_id=nautilus_pyo3.UUID4(),
        ts_init=0,
        cancel_reason="Insufficient balance",
    )

    try:
        # Act
        client._handle_order_status_report_pyo3(pyo3_report)

        # Assert
        assert msgbus.sent_count > 0
    finally:
        await client._disconnect()


@pytest.mark.asyncio
async def test_handle_fill_report_pyo3(
    exec_client_builder_spot,
    monkeypatch,
    instrument,
    cache,
    msgbus,
):
    # Arrange
    client, ws_client, http_client, instrument_provider = exec_client_builder_spot(
        monkeypatch,
    )
    await client._connect()

    order = LimitOrder(
        trader_id=TestIdStubs.trader_id(),
        strategy_id=TestIdStubs.strategy_id(),
        instrument_id=instrument.id,
        client_order_id=ClientOrderId("O-123456"),
        order_side=OrderSide.BUY,
        quantity=Quantity.from_str("0.100"),
        price=Price.from_str("50000.0"),
        init_id=TestIdStubs.uuid(),
        ts_init=0,
    )
    cache.add_order(order, None)

    fill_report = nautilus_pyo3.FillReport(
        account_id=nautilus_pyo3.AccountId("KRAKEN-UNIFIED"),
        instrument_id=nautilus_pyo3.InstrumentId.from_str(instrument.id.value),
        venue_order_id=nautilus_pyo3.VenueOrderId("KRAKEN-789"),
        trade_id=nautilus_pyo3.TradeId("T-001"),
        order_side=nautilus_pyo3.OrderSide.BUY,
        last_qty=nautilus_pyo3.Quantity.from_str("0.100"),
        last_px=nautilus_pyo3.Price.from_str("50000.0"),
        commission=nautilus_pyo3.Money.from_str("0.10 USDT"),
        liquidity_side=nautilus_pyo3.LiquiditySide.TAKER,
        ts_event=0,
        client_order_id=nautilus_pyo3.ClientOrderId("O-123456"),
        report_id=nautilus_pyo3.UUID4(),
        ts_init=0,
    )

    try:
        # Act
        client._handle_fill_report_pyo3(fill_report)

        # Assert
        assert msgbus.sent_count > 0
    finally:
        await client._disconnect()


@pytest.mark.asyncio
async def test_handle_account_state_pyo3(
    exec_client_builder_spot,
    monkeypatch,
    msgbus,
):
    # Arrange
    client, ws_client, http_client, instrument_provider = exec_client_builder_spot(
        monkeypatch,
    )
    await client._connect()

    account_state = nautilus_pyo3.AccountState(
        account_id=nautilus_pyo3.AccountId("KRAKEN-UNIFIED"),
        account_type=nautilus_pyo3.AccountType.CASH,
        base_currency=None,
        balances=[
            nautilus_pyo3.AccountBalance(
                total=nautilus_pyo3.Money.from_str("100000 USDT"),
                locked=nautilus_pyo3.Money.from_str("0 USDT"),
                free=nautilus_pyo3.Money.from_str("100000 USDT"),
            ),
        ],
        margins=[],
        is_reported=True,
        event_id=nautilus_pyo3.UUID4(),
        ts_event=0,
        ts_init=0,
    )

    try:
        # Act
        client._handle_account_state(account_state)

        # Assert
        assert msgbus.sent_count > 0
    finally:
        await client._disconnect()


# ============================================================================
# EDGE CASE TESTS
# ============================================================================


@pytest.mark.asyncio
async def test_cancel_order_not_in_cache(
    exec_client_builder_spot,
    monkeypatch,
    instrument,
):
    # Arrange
    client, ws_client, http_client, instrument_provider = exec_client_builder_spot(
        monkeypatch,
    )
    await client._connect()

    command = CancelOrder(
        trader_id=TestIdStubs.trader_id(),
        strategy_id=TestIdStubs.strategy_id(),
        instrument_id=instrument.id,
        client_order_id=ClientOrderId("O-NOT-IN-CACHE"),
        venue_order_id=None,
        command_id=TestIdStubs.uuid(),
        ts_init=0,
        client_id=None,
    )

    try:
        # Act - Should handle gracefully without raising
        await client._cancel_order(command)

        # Assert - Cancel should NOT be called (order not found)
        http_client.cancel_order.assert_not_awaited()
    finally:
        await client._disconnect()


@pytest.mark.asyncio
async def test_submit_order_logs_warning_when_instrument_not_found(
    exec_client_builder_spot,
    monkeypatch,
    cache,
):
    # Arrange
    client, ws_client, http_client, instrument_provider = exec_client_builder_spot(
        monkeypatch,
    )
    await client._connect()

    # Create an order with an instrument not in the system
    unknown_instrument_id = InstrumentId(Symbol("UNKNOWN/USDT"), KRAKEN_VENUE)

    order = LimitOrder(
        trader_id=TestIdStubs.trader_id(),
        strategy_id=TestIdStubs.strategy_id(),
        instrument_id=unknown_instrument_id,
        client_order_id=ClientOrderId("O-123456"),
        order_side=OrderSide.BUY,
        quantity=Quantity.from_str("0.100"),
        price=Price.from_str("50000.0"),
        init_id=TestIdStubs.uuid(),
        ts_init=0,
    )

    command = SubmitOrder(
        trader_id=order.trader_id,
        strategy_id=order.strategy_id,
        order=order,
        command_id=TestIdStubs.uuid(),
        ts_init=0,
        position_id=None,
        client_id=None,
    )

    try:
        # Act - Should handle gracefully (HTTP client will be called but may fail)
        await client._submit_order(command)
    finally:
        await client._disconnect()


@pytest.mark.asyncio
async def test_handle_order_rejected_pyo3_conversion(
    exec_client_builder_spot,
    monkeypatch,
    instrument,
    msgbus,
):
    # Arrange
    client, ws_client, http_client, instrument_provider = exec_client_builder_spot(
        monkeypatch,
    )
    await client._connect()

    pyo3_event = nautilus_pyo3.OrderRejected(
        trader_id=nautilus_pyo3.TraderId(TestIdStubs.trader_id().value),
        strategy_id=nautilus_pyo3.StrategyId(TestIdStubs.strategy_id().value),
        instrument_id=nautilus_pyo3.InstrumentId.from_str(instrument.id.value),
        client_order_id=nautilus_pyo3.ClientOrderId("O-123456"),
        account_id=nautilus_pyo3.AccountId(TestIdStubs.account_id().value),
        reason="InsufficientBalance",
        event_id=nautilus_pyo3.UUID4(),
        ts_event=123456789,
        ts_init=123456789,
        reconciliation=False,
    )

    try:
        # Act - Should not raise
        client._handle_order_rejected_pyo3(pyo3_event)

        # Assert - Event should be converted and sent
        assert msgbus.sent_count > 0
    finally:
        await client._disconnect()


@pytest.mark.asyncio
async def test_handle_order_cancel_rejected_pyo3_conversion(
    exec_client_builder_spot,
    monkeypatch,
    instrument,
    msgbus,
):
    # Arrange
    client, ws_client, http_client, instrument_provider = exec_client_builder_spot(
        monkeypatch,
    )
    await client._connect()

    pyo3_event = nautilus_pyo3.OrderCancelRejected(
        trader_id=nautilus_pyo3.TraderId(TestIdStubs.trader_id().value),
        strategy_id=nautilus_pyo3.StrategyId(TestIdStubs.strategy_id().value),
        instrument_id=nautilus_pyo3.InstrumentId.from_str(instrument.id.value),
        client_order_id=nautilus_pyo3.ClientOrderId("O-123456"),
        venue_order_id=nautilus_pyo3.VenueOrderId("KRAKEN-12345"),
        reason="OrderNotFound",
        event_id=nautilus_pyo3.UUID4(),
        ts_event=123456789,
        ts_init=123456789,
        reconciliation=False,
        account_id=nautilus_pyo3.AccountId(TestIdStubs.account_id().value),
    )

    try:
        # Act - Should not raise
        client._handle_order_cancel_rejected_pyo3(pyo3_event)

        # Assert - Event should be converted and sent
        assert msgbus.sent_count > 0
    finally:
        await client._disconnect()


@pytest.mark.asyncio
async def test_external_order_fill_report_sent_directly(
    exec_client_builder_spot,
    monkeypatch,
    instrument,
    cache,
    msgbus,
):
    # Arrange
    client, ws_client, http_client, instrument_provider = exec_client_builder_spot(
        monkeypatch,
    )
    await client._connect()

    # Note: Do NOT add order to cache - this simulates an external order

    fill_report = nautilus_pyo3.FillReport(
        account_id=nautilus_pyo3.AccountId("KRAKEN-UNIFIED"),
        instrument_id=nautilus_pyo3.InstrumentId.from_str(instrument.id.value),
        venue_order_id=nautilus_pyo3.VenueOrderId("KRAKEN-EXTERNAL-789"),
        trade_id=nautilus_pyo3.TradeId("T-EXT-001"),
        order_side=nautilus_pyo3.OrderSide.BUY,
        last_qty=nautilus_pyo3.Quantity.from_str("0.100"),
        last_px=nautilus_pyo3.Price.from_str("50000.0"),
        commission=nautilus_pyo3.Money.from_str("0.10 USDT"),
        liquidity_side=nautilus_pyo3.LiquiditySide.TAKER,
        ts_event=0,
        client_order_id=nautilus_pyo3.ClientOrderId("EXTERNAL-ORDER"),
        report_id=nautilus_pyo3.UUID4(),
        ts_init=0,
    )

    try:
        # Act - Should handle external order gracefully
        client._handle_fill_report_pyo3(fill_report)

        # Assert - External orders should be sent as fill reports
        assert msgbus.sent_count > 0
    finally:
        await client._disconnect()
