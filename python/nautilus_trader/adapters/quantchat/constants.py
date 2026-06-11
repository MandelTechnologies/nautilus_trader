# -------------------------------------------------------------------------------------------------
#  QuantChat Local Paper Trading Adapter for Nautilus Trader
#  https://github.com/mandeltechnologies/quantchat.com
# -------------------------------------------------------------------------------------------------

from __future__ import annotations

from nautilus_trader.model.data import BarSpecification
from nautilus_trader.model.enums import BarAggregation
from nautilus_trader.model.identifiers import AccountId
from nautilus_trader.model.identifiers import Venue


# Venue identifier for local paper trading
QUANTCHAT_VENUE = Venue("QUANTCHAT")

# Paper account identifier. Deterministic so positions restored at boot reference
# the same account the execution client registers on every container (re)start.
QUANTCHAT_PAPER_ACCOUNT_ID = AccountId("QUANTCHAT-PAPER-001")

# Default Redis channel prefixes
REDIS_BAR_CHANNEL_PREFIX = "market:bar:"
REDIS_QUOTE_CHANNEL_PREFIX = "market:quote:"
# Model predictions published by the backend signal worker, one channel per
# model version: model:signal:{modelVersionId}.
REDIS_MODEL_SIGNAL_CHANNEL_PREFIX = "model:signal:"

# Local msgbus topic the data client republishes model signals on; the intent
# strategy subscribes and feeds its evaluation-time signal store.
MODEL_SIGNAL_TOPIC = "data.quantchat.model_signal"

# Default configuration values
DEFAULT_REDIS_URL = "redis://localhost:6379"
DEFAULT_STARTING_BALANCE = "100000 USD"
DEFAULT_BASE_LATENCY_MS = 50
DEFAULT_SLIPPAGE_BPS = 5.0

# Platform timeframe strings keyed by (step, aggregation).
_TIMEFRAME_BY_SPEC = {
    (1, BarAggregation.MINUTE): "1m",
    (5, BarAggregation.MINUTE): "5m",
    (15, BarAggregation.MINUTE): "15m",
    (30, BarAggregation.MINUTE): "30m",
    (1, BarAggregation.HOUR): "1h",
    (1, BarAggregation.DAY): "1d",
}


def bar_spec_timeframe(spec: BarSpecification) -> str | None:
    """
    Map a Nautilus bar specification to the platform timeframe string ("1m", "5m", ...).
    """
    return _TIMEFRAME_BY_SPEC.get((spec.step, spec.aggregation))


def bar_channel(symbol: str, timeframe: str) -> str:
    """
    Redis pub/sub channel carrying finalized bars for a symbol and timeframe.

    Must match the channel format published by the backend market-data writer
    (backend-rs `publish_bars_to_redis`).

    """
    return f"{REDIS_BAR_CHANNEL_PREFIX}{symbol}:{timeframe}"
