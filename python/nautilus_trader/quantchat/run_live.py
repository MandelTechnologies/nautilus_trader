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
    redis_url = config.get("redisUrl") or config.get("redis_url") or "redis://localhost:6379"

    provider = QuantChatInstrumentProvider(
        clock=LiveClock(),
        config=InstrumentProviderConfig(load_all=False),
    )
    instrument = provider._create_instrument(symbol)
    bar_type = BarType.from_str(
        f"{instrument.id}-{_timeframe_to_bar_type_suffix(timeframe)}-LAST-EXTERNAL",
    )
    runtime = QuantChatRuntime(
        instrument_id=instrument.id,
        bar_type=bar_type,
        symbol=symbol,
        timeframe=timeframe,
        base_currency=runtime_config.get("baseCurrency", "USD"),
        start_time=runtime_config.get("startTime", ""),
    )

    strategy = build_intent_strategy(runtime, compiled_plan, parameters)
    virtual_cash = config.get("virtualCash", config.get("initialCapital", 100000))
    bot_id = str(config.get("botId", "BOT")).replace("-", "")[:12]

    node_config = TradingNodeConfig(
        trader_id=TraderId(f"QUANTCHAT-{bot_id or 'BOT'}"),
        data_clients={
            QUANTCHAT: QuantChatDataClientConfig(
                redis_url=redis_url,
                symbols=[symbol],
                can_access_tick_data=bool(config.get("canAccessTickData", False)),
            ),
        },
        exec_clients={
            QUANTCHAT: QuantChatExecClientConfig(
                redis_url=redis_url,
                starting_balance=f"{virtual_cash} USD",
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
