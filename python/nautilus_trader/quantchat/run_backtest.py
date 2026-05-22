from __future__ import annotations

from datetime import UTC
from datetime import datetime
from decimal import ROUND_DOWN
from decimal import Decimal
from typing import Any

from nautilus_trader.adapters.quantchat.constants import QUANTCHAT_VENUE
from nautilus_trader.adapters.quantchat.providers import QuantChatInstrumentProvider
from nautilus_trader.backtest.config import BacktestEngineConfig
from nautilus_trader.backtest.engine import BacktestEngine
from nautilus_trader.backtest.models import FillModel
from nautilus_trader.backtest.models import MakerTakerFeeModel
from nautilus_trader.common.component import LiveClock
from nautilus_trader.config import InstrumentProviderConfig
from nautilus_trader.config import LoggingConfig
from nautilus_trader.core.datetime import dt_to_unix_nanos
from nautilus_trader.data.config import DataEngineConfig
from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import BarType
from nautilus_trader.model.enums import AccountType
from nautilus_trader.model.enums import OmsType
from nautilus_trader.model.identifiers import TraderId
from nautilus_trader.model.objects import Currency
from nautilus_trader.model.objects import Money
from nautilus_trader.model.objects import Quantity
from nautilus_trader.quantchat.intent_strategy import QuantChatRuntime
from nautilus_trader.quantchat.intent_strategy import build_intent_strategy


def _parse_time(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def _timeframe_to_bar_type_suffix(timeframe: str) -> str:
    mapping = {
        "1m": "1-MINUTE",
        "5m": "5-MINUTE",
        "15m": "15-MINUTE",
        "30m": "30-MINUTE",
        "1h": "1-HOUR",
        "1d": "1-DAY",
    }
    if timeframe not in mapping:
        raise ValueError(f"Unsupported timeframe: {timeframe}")
    return mapping[timeframe]


def _dataframe_records(value: Any) -> list[dict[str, Any]]:
    if value is None:
        return []
    if hasattr(value, "to_dict"):
        records = value.to_dict(orient="records")
        return [_jsonable_record(record) for record in records]
    return []


def _jsonable_record(record: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, value in record.items():
        if isinstance(value, Decimal):
            out[str(key)] = float(value)
        elif hasattr(value, "as_double"):
            out[str(key)] = value.as_double()
        else:
            out[str(key)] = (
                str(value) if not isinstance(value, (str, int, float, bool, type(None))) else value
            )
    return out


def _summary_from_reports(
    initial_capital: float,
    bars: list[dict[str, Any]],
    fills: list[dict[str, Any]],
    account: list[dict[str, Any]],
) -> dict[str, Any]:
    ending_equity = initial_capital
    if account:
        last = account[-1]
        for key in ("total", "balance_total", "cash", "free"):
            if key in last:
                try:
                    ending_equity = float(str(last[key]).split()[0])
                    break
                except (IndexError, TypeError, ValueError):
                    continue

    net_return_pct = (
        ((ending_equity - initial_capital) / initial_capital) * 100 if initial_capital else 0
    )
    return {
        "startingEquity": initial_capital,
        "endingEquity": ending_equity,
        "netReturnPct": net_return_pct,
        "totalTrades": len(fills),
        "barsProcessed": len(bars),
        "metricVersion": "quantchat_backtest_metrics_v1",
    }


def _make_bar_volume(instrument: Any, value: Any) -> Quantity:
    precision = int(instrument.size_precision)
    decimal_value = Decimal(str(value))
    if decimal_value <= 0:
        return Quantity(0, precision=precision)

    quantum = Decimal(1).scaleb(-precision)
    rounded = decimal_value.quantize(quantum, rounding=ROUND_DOWN)
    return Quantity(rounded, precision=precision)


def run_backtest_plan(config: dict[str, Any]) -> dict[str, Any]:
    runtime_config = config["runtimeBindings"]
    simulation = config["simulationSettings"]
    bars_payload = config["bars"]
    parameters = config.get("effectiveParameters", {})
    compiled_plan = config.get("compiledPlan")
    if not isinstance(compiled_plan, dict) or not compiled_plan:
        raise ValueError("compiledPlan is required for backtesting")

    provider = QuantChatInstrumentProvider(
        clock=LiveClock(),
        config=InstrumentProviderConfig(load_all=False),
    )
    symbol = runtime_config["instrumentSymbol"]
    fee_rate = Decimal(str(simulation.get("feesBps", 0) or 0)) / Decimal("10000")
    instrument = provider._create_instrument(symbol, maker_fee=fee_rate, taker_fee=fee_rate)

    timeframe = runtime_config["timeframe"]
    bar_type = BarType.from_str(
        f"{instrument.id}-{_timeframe_to_bar_type_suffix(timeframe)}-LAST-EXTERNAL",
    )

    engine = BacktestEngine(
        config=BacktestEngineConfig(
            trader_id=TraderId("QUANTCHAT-BACKTEST-001"),
            data_engine=DataEngineConfig(
                time_bars_interval_type="left-open",
                time_bars_timestamp_on_close=True,
                time_bars_skip_first_non_full_bar=False,
                time_bars_build_with_no_updates=False,
                validate_data_sequence=True,
            ),
            logging=LoggingConfig(log_level="INFO"),
        ),
    )

    initial_capital = float(runtime_config["initialCapital"])
    base_currency = Currency.from_str(runtime_config.get("baseCurrency", "USD"))
    fill_model = FillModel(
        prob_fill_on_limit=1.0,
        prob_slippage=1.0 if float(simulation.get("slippageBps", 0) or 0) > 0 else 0.0,
        random_seed=int(runtime_config.get("runSeed", 1)),
    )

    engine.add_venue(
        venue=QUANTCHAT_VENUE,
        oms_type=OmsType.NETTING,
        account_type=AccountType.MARGIN,
        base_currency=base_currency,
        starting_balances=[Money(initial_capital, base_currency)],
        fill_model=fill_model,
        fee_model=MakerTakerFeeModel(),
    )
    engine.add_instrument(instrument)

    bars: list[Bar] = []
    for item in bars_payload:
        ts = dt_to_unix_nanos(_parse_time(item["timestamp"]))
        bars.append(
            Bar(
                bar_type=bar_type,
                open=instrument.make_price(item["open"]),
                high=instrument.make_price(item["high"]),
                low=instrument.make_price(item["low"]),
                close=instrument.make_price(item["close"]),
                volume=_make_bar_volume(instrument, item["volume"]),
                ts_event=ts,
                ts_init=ts,
            ),
        )
    engine.add_data(bars)

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
    engine.add_strategy(strategy)
    engine.run()

    fills = _dataframe_records(engine.trader.generate_order_fills_report())
    positions = _dataframe_records(engine.trader.generate_positions_report())
    account = _dataframe_records(engine.trader.generate_account_report(QUANTCHAT_VENUE))

    summary = _summary_from_reports(initial_capital, bars_payload, fills, account)
    engine.dispose()

    return {
        "summaryMetrics": summary,
        "artifactManifest": {
            "orders": [],
            "fills": fills,
            "positions": positions,
            "account": account,
            "bars": {
                "count": len(bars_payload),
                "firstTimestamp": bars_payload[0]["timestamp"] if bars_payload else None,
                "lastTimestamp": bars_payload[-1]["timestamp"] if bars_payload else None,
            },
            "simulationSettingsApplied": {
                "feesBps": simulation.get("feesBps", 0),
                "slippageBps": simulation.get("slippageBps", 0),
                "fillModel": simulation.get("fillModel", "next_bar_open"),
            },
        },
    }
