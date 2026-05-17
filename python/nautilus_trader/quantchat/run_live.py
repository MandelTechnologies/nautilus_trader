from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
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
from nautilus_trader.quantchat.run_backtest import _timeframe_to_bar_type_suffix
from nautilus_trader.quantchat.strategy_loader import execute_strategy_module


@dataclass(frozen=True)
class QuantChatLiveRuntime:
    instrument_id: Any
    bar_type: BarType
    symbol: str
    timeframe: str

    def __getitem__(self, key: str) -> Any:
        return getattr(self, key)


def run_live_strategy_module(strategy_path: str | Path, config: dict[str, Any]) -> None:
    runtime_config = config.get("runtimeBindings", {})
    parameters = config.get("effectiveParameters", config.get("parameters", {}))
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
    runtime = QuantChatLiveRuntime(
        instrument_id=instrument.id,
        bar_type=bar_type,
        symbol=symbol,
        timeframe=timeframe,
    )

    module = execute_strategy_module(strategy_path, module_name="quantchat_generated_strategy")
    build_strategy = getattr(module, "build_strategy", None)
    if build_strategy is None:
        raise ValueError("Strategy module is missing build_strategy(runtime, parameters)")

    strategy = build_strategy(runtime, parameters)
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
