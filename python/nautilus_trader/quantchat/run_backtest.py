from __future__ import annotations

from datetime import UTC
from datetime import datetime
from decimal import ROUND_DOWN
from decimal import Decimal
from itertools import pairwise
from typing import Any

from nautilus_trader.adapters.quantchat.constants import QUANTCHAT_VENUE
from nautilus_trader.adapters.quantchat.providers import QuantChatInstrumentProvider
from nautilus_trader.backtest.config import BacktestEngineConfig
from nautilus_trader.backtest.engine import BacktestEngine
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


def _jsonable_value(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if hasattr(value, "as_double"):
        return value.as_double()
    if isinstance(value, list):
        return [_jsonable_value(item) for item in value]
    if isinstance(value, (str, int, float, bool, type(None))):
        return value
    return str(value)


def _jsonable_record(record: dict[str, Any]) -> dict[str, Any]:
    return {str(key): _jsonable_value(value) for key, value in record.items()}


def _money_amount(value: Any) -> float:
    """
    Parse the amount from a "123.45 USD" money string (0.0 when unparsable).
    """
    try:
        return float(str(value).split()[0])
    except (IndexError, TypeError, ValueError):
        return 0.0


def _parse_fill_events(fills: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Convert filled-order report records into replayable fill events sorted by time.
    """
    events = []
    for record in fills:
        commissions = record.get("commissions") or []
        events.append(
            {
                "ts": _parse_time(str(record["ts_last"])),
                "is_buy": str(record.get("side", "")).upper() == "BUY",
                "qty": float(str(record["filled_qty"])),
                "px": float(record["avg_px"]),
                "commission": sum(_money_amount(c) for c in commissions),
            },
        )
    events.sort(key=lambda event: event["ts"])
    return events


def _equity_curve(
    initial_capital: float,
    bars: list[dict[str, Any]],
    events: list[dict[str, Any]],
    window_start: datetime,
) -> tuple[list[tuple[datetime, float]], float, float, float | None]:
    """
    Replay fill events against the bar series, marking equity as cash + position * close
    on every bar in the requested window.

    Returns the curve plus the replayed ending cash, position quantity, and the last in-
    window close.

    """
    cash = initial_capital
    qty = 0.0
    curve: list[tuple[datetime, float]] = []
    index = 0
    last_close = None
    for bar in bars:
        bar_ts = _parse_time(str(bar["timestamp"]))
        while index < len(events) and events[index]["ts"] <= bar_ts:
            event = events[index]
            notional = event["qty"] * event["px"]
            if event["is_buy"]:
                cash -= notional + event["commission"]
                qty += event["qty"]
            else:
                cash += notional - event["commission"]
                qty -= event["qty"]
            index += 1
        if bar_ts >= window_start:
            last_close = float(bar["close"])
            curve.append((bar_ts, cash + qty * last_close))
    return curve, cash, qty, last_close


def _annualized_ratios(
    returns: list[float],
    span_seconds: float,
) -> tuple[float | None, float | None]:
    """
    Return (sharpe, sortino) annualized by the observed bar frequency, or None where
    undefined (too few returns, zero variance, or no downside).
    """
    if not returns or span_seconds <= 0:
        return None, None
    periods_per_year = len(returns) / (span_seconds / (365.25 * 86_400.0))
    mean_return = sum(returns) / len(returns)
    annualizer = periods_per_year**0.5

    sharpe = None
    if len(returns) > 1:
        variance = sum((r - mean_return) ** 2 for r in returns) / (len(returns) - 1)
        std = variance**0.5
        if std > 0:
            sharpe = mean_return / std * annualizer

    sortino = None
    downside = (sum(min(r, 0.0) ** 2 for r in returns) / len(returns)) ** 0.5
    if downside > 0:
        sortino = mean_return / downside * annualizer
    return sharpe, sortino


def _summary_metrics(
    initial_capital: float,
    bars: list[dict[str, Any]],
    fills: list[dict[str, Any]],
    account: list[dict[str, Any]],
    window_start: datetime,
    quote_currency: str,
) -> dict[str, Any]:
    """
    Compute summary metrics from a bar-close equity curve.

    The curve replays the engine's own fills (quantities, prices, and commissions), so
    it matches the engine's accounting. Ending cash prefers the engine's account report,
    making ending equity exact; the curve feeds the return-based metrics.

    """
    curve, cash, qty, last_close = _equity_curve(
        initial_capital,
        bars,
        _parse_fill_events(fills),
        window_start,
    )

    # The account is multi-currency (rows per currency per event); ending cash is
    # the engine's last quote-currency balance.
    ending_cash = cash
    cash_rows = [row for row in account if str(row.get("total", "")).endswith(f" {quote_currency}")]
    if cash_rows:
        ending_cash = _money_amount(cash_rows[-1]["total"])
    ending_equity = ending_cash + (qty * last_close if last_close is not None else 0.0)

    equities = [initial_capital] + [equity for _, equity in curve]
    returns = [
        (current / previous) - 1.0 for previous, current in pairwise(equities) if previous > 0
    ]

    max_drawdown_pct = 0.0
    peak = initial_capital
    for equity in equities:
        peak = max(peak, equity)
        if peak > 0:
            max_drawdown_pct = max(max_drawdown_pct, (peak - equity) / peak * 100.0)

    span_seconds = (curve[-1][0] - window_start).total_seconds() if curve else 0.0
    sharpe, sortino = _annualized_ratios(returns, span_seconds)

    net_return_pct = (
        ((ending_equity - initial_capital) / initial_capital) * 100 if initial_capital else 0.0
    )
    return {
        "startingEquity": initial_capital,
        "endingEquity": ending_equity,
        "endingCash": ending_cash,
        "endingPositionValue": qty * last_close if last_close is not None else 0.0,
        "netReturnPct": net_return_pct,
        "sharpeRatio": sharpe,
        "sortinoRatio": sortino,
        "maxDrawdownPct": max_drawdown_pct,
        "totalTrades": len(fills),
        "barsProcessed": len(bars),
        "metricVersion": "quantchat_backtest_metrics_v2",
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
    # Fees and slippage are both transaction costs in bps of fill notional, charged
    # through the venue's taker commission. Paper trading charges the same slippage
    # bps by slipping the fill price; one setting costs the same in both runtimes.
    fees_bps = float(simulation.get("feesBps", 0) or 0)
    slippage_bps = float(simulation.get("slippageBps", 0) or 0)
    cost_rate = Decimal(str(fees_bps + slippage_bps)) / Decimal("10000")
    instrument = provider._create_instrument(symbol, maker_fee=cost_rate, taker_fee=cost_rate)

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

    # A CASH account matches paper trading: buys spend the balance, shorting and
    # borrowing are impossible, and ending equity is cash + open position value.
    # The account is multi-currency (base_currency=None) because Nautilus requires
    # it for spot CurrencyPair instruments; quote-currency flows are identical.
    engine.add_venue(
        venue=QUANTCHAT_VENUE,
        oms_type=OmsType.NETTING,
        account_type=AccountType.CASH,
        base_currency=None,
        starting_balances=[Money(initial_capital, base_currency)],
        fee_model=MakerTakerFeeModel(),
    )
    engine.add_instrument(instrument)

    bars: list[Bar] = []
    model_signals: dict[str, Any] = {}
    for item in bars_payload:
        ts = dt_to_unix_nanos(_parse_time(item["timestamp"]))
        if isinstance(item.get("modelSignals"), dict):
            model_signals[str(ts)] = item["modelSignals"]
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
        model_signals=model_signals,
        cost_bps=fees_bps + slippage_bps,
    )
    strategy = build_intent_strategy(runtime, compiled_plan, parameters)
    engine.add_strategy(strategy)
    engine.run()

    # The engine swallows AccountError (e.g. a balance went negative): it logs,
    # force-stops, and returns normally — which would report a partial run as a
    # complete one. A finished run leaves the engine clock at the final bar.
    last_bar_ts = dt_to_unix_nanos(_parse_time(bars_payload[-1]["timestamp"]))
    if engine.kernel.clock.timestamp_ns() < last_bar_ts:
        raise RuntimeError(
            "Backtest engine stopped before processing the full bar series; "
            "see engine logs for the cause (e.g. an account balance violation)",
        )

    fills = _dataframe_records(engine.trader.generate_order_fills_report())
    positions = _dataframe_records(engine.trader.generate_positions_report())
    account = _dataframe_records(engine.trader.generate_account_report(QUANTCHAT_VENUE))

    window_start = _parse_time(runtime_config["startTime"])
    summary = _summary_metrics(
        initial_capital,
        bars_payload,
        fills,
        account,
        window_start,
        base_currency.code,
    )
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
                "feesBps": fees_bps,
                "slippageBps": slippage_bps,
                "costApplication": (
                    "fees and slippage charged as taker commission; market orders "
                    "cross a one-tick spread around the bar close"
                ),
            },
        },
    }
