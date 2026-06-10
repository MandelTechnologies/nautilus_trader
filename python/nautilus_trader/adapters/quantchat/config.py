# -------------------------------------------------------------------------------------------------
#  QuantChat Local Paper Trading Adapter for Nautilus Trader
#  https://github.com/mandeltechnologies/quantchat.com
# -------------------------------------------------------------------------------------------------

from nautilus_trader.adapters.quantchat.constants import DEFAULT_BASE_LATENCY_MS
from nautilus_trader.adapters.quantchat.constants import DEFAULT_REDIS_URL
from nautilus_trader.adapters.quantchat.constants import DEFAULT_SLIPPAGE_BPS
from nautilus_trader.adapters.quantchat.constants import DEFAULT_STARTING_BALANCE
from nautilus_trader.adapters.quantchat.constants import QUANTCHAT_VENUE
from nautilus_trader.config import LiveDataClientConfig
from nautilus_trader.config import LiveExecClientConfig
from nautilus_trader.config import PositiveInt
from nautilus_trader.model.identifiers import Venue


class QuantChatDataClientConfig(LiveDataClientConfig, frozen=True):
    """
    Configuration for ``QuantChatDataClient`` instances.

    Parameters
    ----------
    venue : Venue, default QUANTCHAT_VENUE
        The venue for the client.
    redis_url : str, default "redis://localhost:6379"
        The Redis connection URL for subscribing to market data.
    symbols : list[str], optional
        List of symbols to subscribe to on startup.
    can_access_tick_data : bool, default False
        Whether the user's membership tier allows tick data access (PRO/ELITE only).

    """

    venue: Venue = QUANTCHAT_VENUE
    redis_url: str = DEFAULT_REDIS_URL
    symbols: list[str] | None = None
    can_access_tick_data: bool = False


class QuantChatExecClientConfig(LiveExecClientConfig, frozen=True):
    """
    Configuration for ``QuantChatExecutionClient`` instances.

    Parameters
    ----------
    venue : Venue, default QUANTCHAT_VENUE
        The venue for the client.
    redis_url : str, default "redis://localhost:6379"
        The Redis connection URL for receiving price data (used for fill simulation).
    starting_balance : str, default "100000 USD"
        The starting balance for the paper trading account.
    base_latency_ms : PositiveInt, default 50
        Base execution latency in milliseconds.
    slippage_bps : float, default 5.0
        Price slippage in basis points of the market price.

    """

    venue: Venue = QUANTCHAT_VENUE
    redis_url: str = DEFAULT_REDIS_URL
    starting_balance: str = DEFAULT_STARTING_BALANCE
    base_latency_ms: PositiveInt = DEFAULT_BASE_LATENCY_MS
    slippage_bps: float = DEFAULT_SLIPPAGE_BPS
