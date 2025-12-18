# -------------------------------------------------------------------------------------------------
#  QuantChat Local Paper Trading Adapter for Nautilus Trader
#  https://github.com/mandeltechnologies/quantchat.com
# -------------------------------------------------------------------------------------------------
"""
QuantChat local paper trading adapter for Nautilus Trader.

Provides data and execution clients for local paper trading with:
- Price data from EODHD via Redis pub/sub
- Simulated order execution with configurable slippage/latency
- Position tracking via the quantchat backend

This adapter allows strategies to be tested with realistic fill simulation
without requiring a live brokerage connection.

Environment Variables
---------------------
REDIS_URL : str
    Redis connection URL (default: redis://localhost:6379).
QUANTCHAT_BOT_ID : str
    Bot ID for event emission (used by EventEmitter actor).

Example
-------
>>> from nautilus_trader.adapters.quantchat import QUANTCHAT
>>> from nautilus_trader.adapters.quantchat import QuantChatDataClientConfig
>>> from nautilus_trader.adapters.quantchat import QuantChatExecClientConfig
>>> from nautilus_trader.adapters.quantchat import QuantChatLiveDataClientFactory
>>> from nautilus_trader.adapters.quantchat import QuantChatLiveExecClientFactory
>>> from nautilus_trader.config import TradingNodeConfig
>>>
>>> config = TradingNodeConfig(
...     data_clients={
...         QUANTCHAT: QuantChatDataClientConfig(
...             redis_url="redis://localhost:6379",
...             symbols=["BTC-USD", "ETH-USD"],
...         ),
...     },
...     exec_clients={
...         QUANTCHAT: QuantChatExecClientConfig(
...             redis_url="redis://localhost:6379",
...             starting_balance="100000 USD",
...         ),
...     },
... )

"""

from nautilus_trader.adapters.quantchat.config import QuantChatDataClientConfig
from nautilus_trader.adapters.quantchat.config import QuantChatExecClientConfig
from nautilus_trader.adapters.quantchat.constants import QUANTCHAT_VENUE
from nautilus_trader.adapters.quantchat.data import QuantChatDataClient
from nautilus_trader.adapters.quantchat.execution import QuantChatExecutionClient
from nautilus_trader.adapters.quantchat.factories import QuantChatLiveDataClientFactory
from nautilus_trader.adapters.quantchat.factories import QuantChatLiveExecClientFactory
from nautilus_trader.adapters.quantchat.fill_model import QuantChatFillModel
from nautilus_trader.adapters.quantchat.providers import QuantChatInstrumentProvider


# Convenience alias
QUANTCHAT = "QUANTCHAT"

__all__ = [
    "QUANTCHAT",
    "QUANTCHAT_VENUE",
    "QuantChatDataClient",
    "QuantChatDataClientConfig",
    "QuantChatExecClientConfig",
    "QuantChatExecutionClient",
    "QuantChatFillModel",
    "QuantChatInstrumentProvider",
    "QuantChatLiveDataClientFactory",
    "QuantChatLiveExecClientFactory",
]
