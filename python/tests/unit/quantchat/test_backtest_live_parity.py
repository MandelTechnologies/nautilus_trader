"""
Backtest/live parity: the same plan over the same bars must produce identical decisions
whether history is streamed through the engine (backtest shape: warmup bars precede the
window and rules are gated to the run window) or seeded from the deploy config at
startup (live boot shape: ``warmup_bars`` in the runtime, only in-window bars arrive by
subscription).

Both sessions run against the same deterministic simulated venue; transport (Redis) and
the paper venue's fill pricing are covered by integration smokes. Requires the compiled
nautilus core.

"""

from datetime import UTC
from datetime import datetime
from datetime import timedelta
from decimal import Decimal
import math

import pytest


pytest.importorskip("nautilus_trader.backtest.engine", reason="requires built nautilus core")

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
from nautilus_trader.quantchat.intent_strategy import QuantChatRuntime
from nautilus_trader.quantchat.intent_strategy import build_intent_strategy
from nautilus_trader.quantchat.run_backtest import _make_bar_volume


WARMUP_BARS = 100
WINDOW_BARS = 300
COST_BPS = 5.0
INITIAL_CAPITAL = 100_000.0
START = datetime(2026, 1, 1, tzinfo=UTC)

PLAN = {
    "runtimeContractVersion": "quantchat_strategy_intent_v4",
    "features": [
        {"id": "sma_fast", "kind": "sma", "field": "close", "period": 5},
        {"id": "sma_slow", "kind": "sma", "field": "close", "period": 20},
        {"id": "ema_20", "kind": "ema", "field": "close", "period": 20},
        {"id": "rsi_14", "kind": "rsi", "field": "close", "period": 14},
        {
            "id": "bb_up",
            "kind": "bollinger",
            "field": "close",
            "period": 20,
            "stdDev": 2.0,
            "band": "upper",
        },
        {"id": "hi_50", "kind": "rolling_high", "field": "close", "period": 50},
    ],
    "startupActions": [{"kind": "buy_fixed_notional", "amount": 5000}],
    "rules": [
        {
            "id": "enter",
            "trigger": {"kind": "bar_close"},
            "conditions": [
                {
                    "kind": "crosses_above",
                    "left": {"kind": "feature", "featureId": "sma_fast"},
                    "right": {"kind": "feature", "featureId": "sma_slow"},
                },
                {
                    "kind": "compare",
                    "op": "<",
                    "left": {"kind": "feature", "featureId": "rsi_14"},
                    "right": {"kind": "constant", "value": 75},
                },
            ],
            "actions": [{"kind": "buy_available_cash_pct", "percent": 0.5}],
        },
        {
            "id": "exit",
            "trigger": {"kind": "bar_close"},
            "conditions": [
                {
                    "kind": "crosses_below",
                    "left": {"kind": "feature", "featureId": "sma_fast"},
                    "right": {"kind": "feature", "featureId": "sma_slow"},
                },
            ],
            "actions": [{"kind": "exit_position"}],
        },
        {
            "id": "breakout",
            "trigger": {"kind": "bar_close"},
            "conditions": [
                {
                    "kind": "compare",
                    "op": ">",
                    "left": {"kind": "bar_field", "field": "close"},
                    "right": {"kind": "feature", "featureId": "hi_50"},
                },
                {
                    "kind": "compare",
                    "op": ">",
                    "left": {"kind": "feature", "featureId": "ema_20"},
                    "right": {"kind": "feature", "featureId": "bb_up"},
                },
            ],
            "actions": [{"kind": "buy_fixed_notional", "amount": 1000}],
        },
    ],
    "risk": {},
}


def _bar_records() -> list[dict[str, float]]:
    records = []
    prev_close = 100.0
    for index in range(WARMUP_BARS + WINDOW_BARS):
        ts = START + timedelta(minutes=index)
        close = 100.0 + 6.0 * math.sin(index / 17.0) + 0.01 * index
        records.append(
            {
                "open": round(prev_close, 2),
                "high": round(max(prev_close, close) + 0.05, 2),
                "low": round(min(prev_close, close) - 0.05, 2),
                "close": round(close, 2),
                "volume": 10.0,
                "ts_event": float(dt_to_unix_nanos(ts)),
            },
        )
        prev_close = close
    return records


def _run_session(
    stream_records: list[dict[str, float]],
    warmup_records: list[dict[str, float]],
) -> dict:
    provider = QuantChatInstrumentProvider(
        clock=LiveClock(),
        config=InstrumentProviderConfig(load_all=False),
    )
    cost_rate = Decimal(str(COST_BPS)) / Decimal("10000")
    instrument = provider._create_instrument(
        "BTC/USD",
        maker_fee=cost_rate,
        taker_fee=cost_rate,
    )
    bar_type = BarType.from_str(f"{instrument.id}-1-MINUTE-LAST-EXTERNAL")

    engine = BacktestEngine(
        config=BacktestEngineConfig(
            trader_id=TraderId("QUANTCHAT-PARITY-001"),
            data_engine=DataEngineConfig(
                time_bars_interval_type="left-open",
                time_bars_timestamp_on_close=True,
                time_bars_skip_first_non_full_bar=False,
                time_bars_build_with_no_updates=False,
                validate_data_sequence=True,
            ),
            logging=LoggingConfig(log_level="ERROR"),
        ),
    )
    engine.add_venue(
        venue=QUANTCHAT_VENUE,
        oms_type=OmsType.NETTING,
        account_type=AccountType.CASH,
        base_currency=None,
        starting_balances=[Money(INITIAL_CAPITAL, Currency.from_str("USD"))],
        fee_model=MakerTakerFeeModel(),
    )
    engine.add_instrument(instrument)
    engine.add_data(
        [
            Bar(
                bar_type=bar_type,
                open=instrument.make_price(record["open"]),
                high=instrument.make_price(record["high"]),
                low=instrument.make_price(record["low"]),
                close=instrument.make_price(record["close"]),
                volume=_make_bar_volume(instrument, record["volume"]),
                ts_event=int(record["ts_event"]),
                ts_init=int(record["ts_event"]),
            )
            for record in stream_records
        ],
    )

    window_start = START + timedelta(minutes=WARMUP_BARS)
    runtime = QuantChatRuntime(
        instrument_id=instrument.id,
        bar_type=bar_type,
        symbol="BTC/USD",
        timeframe="1m",
        start_time=window_start.isoformat(),
        cost_bps=COST_BPS,
        warmup_bars=warmup_records,
    )
    strategy = build_intent_strategy(runtime, PLAN, {})
    engine.add_strategy(strategy)

    events: list[dict] = []
    engine.kernel.msgbus.subscribe("events.quantchat.runtime", events.append)
    engine.run()

    fills = engine.trader.generate_order_fills_report()
    fill_rows = (
        [
            (str(row["ts_last"]), str(row["side"]), str(row["filled_qty"]), float(row["avg_px"]))
            for row in fills.to_dict(orient="records")
        ]
        if fills is not None and len(fills)
        else []
    )
    account = engine.trader.generate_account_report(QUANTCHAT_VENUE)
    usd_totals = [
        str(row["total"])
        for row in account.to_dict(orient="records")
        if str(row.get("currency", "")) == "USD"
    ]
    engine.dispose()
    return {
        "events": events,
        "fills": fill_rows,
        "ending_cash": usd_totals[-1] if usd_totals else None,
    }


def test_streamed_warmup_and_seeded_warmup_make_identical_decisions() -> None:
    records = _bar_records()

    backtest_shaped = _run_session(stream_records=records, warmup_records=[])
    live_shaped = _run_session(
        stream_records=records[WARMUP_BARS:],
        warmup_records=records[:WARMUP_BARS],
    )

    assert backtest_shaped["events"] == live_shaped["events"]
    assert backtest_shaped["fills"] == live_shaped["fills"]
    assert backtest_shaped["ending_cash"] == live_shaped["ending_cash"]

    # Parity over a session that actually did something.
    assert len(backtest_shaped["fills"]) >= 4
    decisions = [e for e in backtest_shaped["events"] if e.get("type") == "decision_evaluated"]
    assert any(e["rule_id"] == "enter" and e["result"] for e in decisions)
    assert any(e["rule_id"] == "exit" and e["result"] for e in decisions)
    assert [e for e in backtest_shaped["events"] if e.get("type") == "startup_actions_completed"]
