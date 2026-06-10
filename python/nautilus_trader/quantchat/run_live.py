from __future__ import annotations

from decimal import Decimal
from typing import Any

from nautilus_trader.adapters.quantchat import QUANTCHAT
from nautilus_trader.adapters.quantchat import QuantChatDataClientConfig
from nautilus_trader.adapters.quantchat import QuantChatExecClientConfig
from nautilus_trader.adapters.quantchat import QuantChatLiveDataClientFactory
from nautilus_trader.adapters.quantchat import QuantChatLiveExecClientFactory
from nautilus_trader.adapters.quantchat.constants import QUANTCHAT_PAPER_ACCOUNT_ID
from nautilus_trader.adapters.quantchat.providers import QuantChatInstrumentProvider
from nautilus_trader.common.component import LiveClock
from nautilus_trader.config import InstrumentProviderConfig
from nautilus_trader.config import LiveDataEngineConfig
from nautilus_trader.config import LiveExecEngineConfig
from nautilus_trader.config import TradingNodeConfig
from nautilus_trader.core.datetime import dt_to_unix_nanos
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.live.node import TradingNode
from nautilus_trader.model.data import BarType
from nautilus_trader.model.enums import LiquiditySide
from nautilus_trader.model.enums import OmsType
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import OrderType
from nautilus_trader.model.events import OrderFilled
from nautilus_trader.model.identifiers import ClientOrderId
from nautilus_trader.model.identifiers import PositionId
from nautilus_trader.model.identifiers import TradeId
from nautilus_trader.model.identifiers import TraderId
from nautilus_trader.model.identifiers import VenueOrderId
from nautilus_trader.model.objects import Currency
from nautilus_trader.model.objects import Money
from nautilus_trader.model.position import Position
from nautilus_trader.quantchat.event_emitter import EventEmitter
from nautilus_trader.quantchat.event_emitter import EventEmitterConfig
from nautilus_trader.quantchat.intent_strategy import QuantChatRuntime
from nautilus_trader.quantchat.intent_strategy import build_intent_strategy
from nautilus_trader.quantchat.run_backtest import _parse_time
from nautilus_trader.quantchat.run_backtest import _timeframe_to_bar_type_suffix


def _parse_warmup_bars(payload: Any) -> list[dict[str, float]]:
    """
    Convert the deploy config's `warmupBars` (backtest bar payload shape) into the float
    records the strategy keeps in its bar history.
    """
    if not isinstance(payload, list):
        return []
    bars = []
    for item in payload:
        bars.append(
            {
                "open": float(item["open"]),
                "high": float(item["high"]),
                "low": float(item["low"]),
                "close": float(item["close"]),
                "volume": float(item["volume"]),
                "ts_event": float(dt_to_unix_nanos(_parse_time(item["timestamp"]))),
            },
        )
    return bars


def _restore_positions(
    node: TradingNode,
    strategy: Any,
    instrument: Any,
    config: dict[str, Any],
) -> None:
    """
    Restore the bot's open position into the engine cache before the node runs.

    The deploy config snapshots positions from the backend ledger. The position is
    inserted silently — built from a reconciliation fill that is never published — so no
    order/position events reach the event emitter and the backend cannot double-count
    state it already holds. The position ID matches the execution engine's netting
    convention (`{instrument_id}-{strategy_id}`) so live fills net against the restored
    position.

    """
    positions = config.get("positions") or []
    for item in positions:
        quantity = Decimal(str(item.get("quantity", "0")))
        average_cost = Decimal(str(item.get("averageCost", "0")))
        symbol = str(item.get("symbol", ""))
        if symbol != instrument.id.symbol.value:
            raise ValueError(
                f"Position symbol {symbol!r} does not match bot instrument "
                f"{instrument.id.symbol.value!r}",
            )
        if quantity <= 0 or average_cost <= 0:
            raise ValueError(f"Invalid position snapshot: qty={quantity} avg={average_cost}")

        fill = OrderFilled(
            trader_id=node.trader_id,
            strategy_id=strategy.id,
            instrument_id=instrument.id,
            client_order_id=ClientOrderId(f"RESTORE-{UUID4().value[:12]}"),
            venue_order_id=VenueOrderId(f"RESTORE-{UUID4().value[:12]}"),
            account_id=QUANTCHAT_PAPER_ACCOUNT_ID,
            trade_id=TradeId(f"RESTORE-{UUID4().value[:12]}"),
            position_id=PositionId(f"{instrument.id}-{strategy.id}"),
            order_side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            last_qty=instrument.make_qty(quantity),
            last_px=instrument.make_price(average_cost),
            currency=Currency.from_str("USD"),
            commission=Money(0, Currency.from_str("USD")),
            liquidity_side=LiquiditySide.TAKER,
            event_id=UUID4(),
            ts_event=0,
            ts_init=0,
            reconciliation=True,
        )
        position = Position(instrument=instrument, fill=fill)
        node.kernel.cache.add_position(position, OmsType.NETTING)
        print(
            f"[INFO] [Trading Node] Restored position: {quantity} {symbol} "
            f"@ {average_cost} (id={position.id})",
        )


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
        startup_actions_completed=bool(runtime_config.get("startupActionsCompleted", False)),
        trades_today=int(runtime_config.get("tradesToday", 0)),
        warmup_bars=_parse_warmup_bars(config.get("warmupBars")),
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
        # There is no external venue to reconcile against: the paper venue's state
        # IS the deploy config, restored below before the node runs.
        exec_engine=LiveExecEngineConfig(reconciliation=False),
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

    _restore_positions(node, strategy, instrument, config)

    try:
        node.run()
    finally:
        node.dispose()
