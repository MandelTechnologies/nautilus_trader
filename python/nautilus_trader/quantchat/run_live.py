from __future__ import annotations

from typing import Any

from nautilus_trader.adapters.quantchat import QUANTCHAT
from nautilus_trader.adapters.quantchat import QuantChatDataClientConfig
from nautilus_trader.adapters.quantchat import QuantChatExecClientConfig
from nautilus_trader.adapters.quantchat import QuantChatLiveDataClientFactory
from nautilus_trader.adapters.quantchat import QuantChatLiveExecClientFactory
from nautilus_trader.adapters.quantchat.providers import QuantChatInstrumentProvider
from nautilus_trader.common.component import LiveClock
from nautilus_trader.config import InstrumentProviderConfig
from nautilus_trader.config import LiveDataEngineConfig
from nautilus_trader.config import TradingNodeConfig
from nautilus_trader.live.node import TradingNode
from nautilus_trader.model.data import BarType
from nautilus_trader.model.identifiers import TraderId
from nautilus_trader.quantchat.event_emitter import EventEmitter
from nautilus_trader.quantchat.event_emitter import EventEmitterConfig
from nautilus_trader.quantchat.intent_strategy import QuantChatRuntime
from nautilus_trader.quantchat.intent_strategy import build_intent_strategy
from nautilus_trader.quantchat.run_backtest import _timeframe_to_bar_type_suffix


def run_live_strategy_plan(config: dict[str, Any]) -> None:
    runtime_config = config.get("runtimeBindings", {})
    parameters = config.get("effectiveParameters", config.get("parameters", {}))
    compiled_plan = config.get("compiledPlan")
    if not isinstance(compiled_plan, dict) or not compiled_plan:
        raise ValueError("compiledPlan is required for live strategy execution")
    symbol = runtime_config.get("instrumentSymbol") or runtime_config.get("symbol")
    if not symbol:
        raise ValueError("Live runtime is missing instrumentSymbol")

    timeframe = runtime_config.get("timeframe", "1m")
    feed_timeframe = runtime_config.get("feedTimeframe", "1m")
    redis_url = config.get("redisUrl") or config.get("redis_url") or "redis://localhost:6379"

    provider = QuantChatInstrumentProvider(
        clock=LiveClock(),
        config=InstrumentProviderConfig(load_all=False),
    )
    instrument = provider._create_instrument(symbol)

    # The backend relays the instrument's ingest feed (1m or 5m). When the strategy
    # timeframe is coarser, the DataEngine aggregates feed bars into composite bars.
    if timeframe == feed_timeframe:
        bar_type = BarType.from_str(
            f"{instrument.id}-{_timeframe_to_bar_type_suffix(timeframe)}-LAST-EXTERNAL",
        )
    else:
        bar_type = BarType.from_str(
            f"{instrument.id}-{_timeframe_to_bar_type_suffix(timeframe)}-LAST-INTERNAL"
            f"@{_timeframe_to_bar_type_suffix(feed_timeframe)}-EXTERNAL",
        )
    runtime = QuantChatRuntime(
        instrument_id=instrument.id,
        bar_type=bar_type,
        symbol=symbol,
        timeframe=timeframe,
        base_currency=runtime_config.get("baseCurrency", "USD"),
        start_time=runtime_config.get("startTime", ""),
        end_time=runtime_config.get("endTime", ""),
        market_calendar=runtime_config.get("marketCalendar", {}),
    )

    strategy = build_intent_strategy(runtime, compiled_plan, parameters)
    virtual_cash = config.get("virtualCash", config.get("initialCapital", 100000))
    bot_id = str(config.get("botId", "BOT")).replace("-", "")[:12]

    node_config = TradingNodeConfig(
        trader_id=TraderId(f"QUANTCHAT-{bot_id or 'BOT'}"),
        # Composite bars must match the platform's stored-bar conventions so live decisions
        # reproduce backtests: timestamps are bucket-start and a feed bar stamped at the
        # bucket boundary belongs to the bucket it opens (right-open). The build delay
        # holds each window open briefly because the final feed bar of a window is only
        # published after that window has ended (ingestor finalize + relay latency).
        data_engine=LiveDataEngineConfig(
            time_bars_interval_type="right-open",
            time_bars_timestamp_on_close=False,
            time_bars_skip_first_non_full_bar=False,
            time_bars_build_with_no_updates=False,
            time_bars_build_delay=2_500_000,  # microseconds
        ),
        data_clients={
            QUANTCHAT: QuantChatDataClientConfig(
                redis_url=redis_url,
                symbols=[symbol],
                can_access_tick_data=bool(config.get("canAccessTickData", False)),
                instrument_provider=InstrumentProviderConfig(
                    load_ids=frozenset([instrument.id]),
                ),
            ),
        },
        exec_clients={
            QUANTCHAT: QuantChatExecClientConfig(
                redis_url=redis_url,
                starting_balance=f"{virtual_cash} USD",
                instrument_provider=InstrumentProviderConfig(
                    load_ids=frozenset([instrument.id]),
                ),
            ),
        },
    )

    node = TradingNode(config=node_config)
    node.trader.add_actor(EventEmitter(EventEmitterConfig(redis_url=redis_url)))
    node.trader.add_strategy(strategy)
    node.add_data_client_factory(QUANTCHAT, QuantChatLiveDataClientFactory)
    node.add_exec_client_factory(QUANTCHAT, QuantChatLiveExecClientFactory)
    node.build()

    try:
        node.run()
    finally:
        node.dispose()
