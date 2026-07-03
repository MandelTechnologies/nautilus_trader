from __future__ import annotations

from datetime import UTC
from datetime import date
from datetime import datetime
from decimal import ROUND_DOWN
from decimal import Decimal
from itertools import pairwise
from math import isfinite
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
        value = float(value)
    elif hasattr(value, "as_double"):
        value = value.as_double()
    if isinstance(value, float):
        # Pandas reports use NaN for not-applicable cells (e.g. duration_ns of an
        # open position); json.dumps would emit a bare NaN, which is not JSON.
        return value if isfinite(value) else None
    if isinstance(value, list):
        return [_jsonable_value(item) for item in value]
    if isinstance(value, (str, int, bool, type(None))):
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


def _orders_report(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Normalize `Trader.generate_orders_report()` records for the artifact manifest.

    The report's per-order dict uses Nautilus's own field name `type` for the order
    type (e.g. "STOP_MARKET"); this is renamed to `orderType` to match the compiled
    plan's own vocabulary so the manifest reads consistently end to end.

    """
    orders = []
    for record in records:
        order = dict(record)
        order["orderType"] = order.pop("type", None)
        orders.append(order)
    return orders


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


def _parse_corporate_actions(payload: Any) -> list[dict[str, Any]]:
    """
    Validate the corporateActions runtime binding into ledger events sorted by ex-date.

    The backend controls the shape; malformed entries fail the run loudly.

    """
    if not isinstance(payload, list):
        raise ValueError("runtimeBindings.corporateActions must be an array")
    actions: list[dict[str, Any]] = []
    for entry in payload:
        kind = str(entry.get("kind", ""))
        try:
            ex_date = date.fromisoformat(str(entry.get("exDate", "")))
            value = float(entry["factor"] if kind == "split" else entry["amount"])
            if kind not in ("split", "dividend"):
                raise ValueError(f"unknown kind: {kind!r}")
        except (KeyError, TypeError, ValueError) as err:
            raise ValueError(f"invalid corporateActions entry {entry!r}: {err}") from err
        pay_date = None
        if kind == "dividend" and entry.get("payDate"):
            pay_date = date.fromisoformat(str(entry["payDate"]))
        actions.append({"ex_date": ex_date, "kind": kind, "value": value, "pay_date": pay_date})
    actions.sort(key=lambda action: action["ex_date"])
    return actions


class _CorporateActionLedger:
    """
    Applies split and dividend ledger effects as bars cross ex-dates.
    """

    def __init__(self, actions: list[dict[str, Any]]) -> None:
        self._actions = actions
        self._index = 0
        self._pending: list[tuple[date, float]] = []
        self.applied = False

    def apply(self, bar_date: date, qty: float, cash: float) -> tuple[float, float]:
        """
        Cross every action whose ex-date this bar reaches and pay dividends that came
        due; returns the updated (qty, cash).
        """
        while (
            self._index < len(self._actions) and self._actions[self._index]["ex_date"] <= bar_date
        ):
            action = self._actions[self._index]
            self._index += 1
            if action["kind"] == "split":
                if qty != 0.0:
                    qty *= action["value"]
                    self.applied = True
            elif qty > 0.0:
                entitlement = qty * action["value"]
                pay_date = action["pay_date"]
                if pay_date is None or pay_date <= action["ex_date"]:
                    cash += entitlement
                else:
                    self._pending.append((pay_date, entitlement))
                self.applied = True
        due = [amount for pay_date, amount in self._pending if pay_date <= bar_date]
        if due:
            cash += sum(due)
            self._pending = [d for d in self._pending if d[0] > bar_date]
        return qty, cash

    def settle_remaining(self, cash: float) -> float:
        """
        Credit entitlements whose pay date falls beyond the series — they still belong
        to the run.
        """
        return cash + sum(amount for _, amount in self._pending)


def _equity_curve(
    initial_capital: float,
    bars: list[dict[str, Any]],
    events: list[dict[str, Any]],
    window_start: datetime,
    corporate_actions: list[dict[str, Any]] | None = None,
) -> tuple[list[tuple[datetime, float]], float, float, float | None, bool]:
    """
    Replay fill events against the bar series, marking equity as cash + position * close
    on every bar in the requested window.

    Corporate actions apply as ledger events (§8.1 accounting plane, on the raw series):
    at a split's ex-date the position quantity multiplies by the ratio; a dividend's
    entitlement is the quantity held entering the ex-date bar and credits cash at the
    pay date (immediately when absent; at series end when the pay date falls beyond the
    window — the entitlement was earned in-window). The venue itself is corporate-
    action-blind, so a strategy that exits after a split under-sells (the venue's book
    still holds the pre-split quantity); the curve reports the economic holding.

    Returns the curve plus the replayed ending cash, position quantity, the last in-
    window close, and whether any corporate-action ledger effect applied.

    """
    cash = initial_capital
    qty = 0.0
    curve: list[tuple[datetime, float]] = []
    index = 0
    last_close = None
    ledger = _CorporateActionLedger(corporate_actions or [])
    for bar in bars:
        bar_ts = _parse_time(str(bar["timestamp"]))
        qty, cash = ledger.apply(bar_ts.date(), qty, cash)
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
    cash = ledger.settle_remaining(cash)
    return curve, cash, qty, last_close, ledger.applied


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
    corporate_actions: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """
    Compute summary metrics from a bar-close equity curve.

    The curve replays the engine's own fills (quantities, prices, and commissions), so
    it matches the engine's accounting. Ending cash prefers the engine's account report,
    making ending equity exact — except when corporate-action ledger effects applied,
    which the engine's account cannot know about; then the replayed cash is the truth.

    """
    curve, cash, qty, last_close, ca_applied = _equity_curve(
        initial_capital,
        bars,
        _parse_fill_events(fills),
        window_start,
        corporate_actions,
    )

    # The account is multi-currency (one row per currency per event, with the
    # currency in its own column); ending cash is the engine's last quote-currency
    # balance, falling back to the replayed cash if the report is empty.
    ending_cash = cash
    cash_rows = [row for row in account if str(row.get("currency", "")) == quote_currency]
    if cash_rows and not ca_applied:
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
    #
    # bar_execution/bar_adaptive_high_low_ordering are pinned explicitly (matching
    # nautilus's own defaults today) rather than left implicit: order-type dispatch
    # (limit/stop/stop_limit/bracket/trailing_stop) depends on the venue synthesizing
    # trade ticks from L1 bars in a fixed O->H->L->C order, and a future nautilus
    # upgrade changing its own defaults must not silently change fill semantics
    # underneath this contract (see intrabarFillPolicy below).
    engine.add_venue(
        venue=QUANTCHAT_VENUE,
        oms_type=OmsType.NETTING,
        account_type=AccountType.CASH,
        base_currency=None,
        starting_balances=[Money(initial_capital, base_currency)],
        fee_model=MakerTakerFeeModel(),
        bar_execution=True,
        bar_adaptive_high_low_ordering=False,
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

    # Predictions ride beside the bars ({rfc3339: {modelVersionId: outputs}});
    # the strategy's store keys on bar ts_event nanoseconds.
    model_signals = {
        str(dt_to_unix_nanos(_parse_time(timestamp))): outputs
        for timestamp, outputs in (config.get("modelSignals") or {}).items()
    }

    runtime = QuantChatRuntime(
        instrument_id=instrument.id,
        bar_type=bar_type,
        symbol=symbol,
        timeframe=timeframe,
        catalyst_symbol=runtime_config.get("catalystSymbol", ""),
        base_currency=runtime_config.get("baseCurrency", "USD"),
        start_time=runtime_config.get("startTime", ""),
        end_time=runtime_config.get("endTime", ""),
        market_calendar=runtime_config.get("marketCalendar", {}),
        catalyst_calendar=runtime_config.get("catalystCalendar", {}),
        model_signals=model_signals,
        cost_bps=fees_bps + slippage_bps,
        corporate_actions=runtime_config.get("corporateActions", []),
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

    orders = _orders_report(_dataframe_records(engine.trader.generate_orders_report()))
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
        _parse_corporate_actions(runtime_config.get("corporateActions", [])),
    )
    engine.dispose()

    return {
        "summaryMetrics": summary,
        "artifactManifest": {
            "orders": orders,
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
                "intrabarFillPolicy": (
                    "resolution rule: fixed O->H->L->C (nautilus default); this is "
                    "conservative for short brackets and optimistic for long brackets "
                    "when a single bar spans both levels"
                ),
            },
        },
    }
