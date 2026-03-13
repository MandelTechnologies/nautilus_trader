# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
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

from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest

from nautilus_trader.adapters.quantchat.config import QuantChatExecClientConfig
from nautilus_trader.adapters.quantchat.execution import QuantChatExecutionClient
from nautilus_trader.adapters.quantchat.providers import QuantChatInstrumentProvider
from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.component import MessageBus
from nautilus_trader.config import InstrumentProviderConfig
from nautilus_trader.portfolio.portfolio import Portfolio
from nautilus_trader.test_kit.stubs.component import TestComponentStubs
from nautilus_trader.test_kit.stubs.identifiers import TestIdStubs


async def _empty_stream():
    if False:
        yield None


@pytest.mark.asyncio
async def test_connect_registers_account_before_emitting_account_state(event_loop, monkeypatch):
    # Arrange
    clock = LiveClock()
    msgbus = MessageBus(
        trader_id=TestIdStubs.trader_id(),
        clock=clock,
    )
    cache = TestComponentStubs.cache()
    Portfolio(
        msgbus=msgbus,
        cache=cache,
        clock=clock,
    )
    provider = QuantChatInstrumentProvider(clock=clock, config=InstrumentProviderConfig())

    fake_pubsub = MagicMock()
    fake_pubsub.listen.return_value = _empty_stream()
    fake_pubsub.unsubscribe = AsyncMock()
    fake_pubsub.close = AsyncMock()

    fake_redis = MagicMock()
    fake_redis.pubsub.return_value = fake_pubsub
    fake_redis.close = AsyncMock()

    monkeypatch.setattr(
        "nautilus_trader.adapters.quantchat.execution.aioredis.from_url",
        MagicMock(return_value=fake_redis),
    )

    client = QuantChatExecutionClient(
        loop=event_loop,
        msgbus=msgbus,
        cache=cache,
        clock=clock,
        instrument_provider=provider,
        config=QuantChatExecClientConfig(),
        name=None,
    )

    # Act / Assert
    try:
        await client._connect()
    finally:
        await client._disconnect()

    fake_pubsub.unsubscribe.assert_awaited_once()
    fake_pubsub.close.assert_awaited_once()
    fake_redis.close.assert_awaited_once()
