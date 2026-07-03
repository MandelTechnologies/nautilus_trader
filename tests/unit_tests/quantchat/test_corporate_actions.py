"""
Corporate actions (composable-signals §8.1): the accounting ledger replay (splits
multiply quantity, dividends credit cash on pay date) and the E2 acceptance story — a
buy-and-hold run through a split and a dividend is total-return correct, and the
adjusted indicator feed keeps a split from firing spurious exits.

Requires the compiled nautilus core and the signal_engine wheel.

"""

from datetime import UTC
from datetime import date
from datetime import datetime
from datetime import timedelta

import pytest


pytest.importorskip("nautilus_trader.backtest.engine", reason="requires built nautilus core")

from nautilus_trader.quantchat.run_backtest import _equity_curve
from nautilus_trader.quantchat.run_backtest import _parse_corporate_actions
from nautilus_trader.quantchat.run_backtest import run_backtest_plan


START = datetime(2026, 1, 5, tzinfo=UTC)
INITIAL_CAPITAL = 100_000.0


def _daily_bars(closes: list[float]) -> list[dict]:
    bars = []
    prev = closes[0]
    for index, close in enumerate(closes):
        ts = START + timedelta(days=index)
        bars.append(
            {
                "timestamp": ts.isoformat(),
                "open": prev,
                "high": max(prev, close),
                "low": min(prev, close),
                "close": close,
                "volume": 1_000.0,
            },
        )
        prev = close
    return bars


def _day(index: int) -> date:
    return (START + timedelta(days=index)).date()


# --- Ledger replay ------------------------------------------------------------


def test_split_multiplies_position_quantity() -> None:
    bars = _daily_bars([100.0] * 5 + [50.0] * 5)
    buy = {"ts": START, "is_buy": True, "qty": 100.0, "px": 100.0, "commission": 0.0}
    split = {"ex_date": _day(5), "kind": "split", "value": 2.0, "pay_date": None}

    curve, cash, qty, last_close, applied = _equity_curve(
        INITIAL_CAPITAL,
        bars,
        [buy],
        START,
        [split],
    )
    assert applied
    assert qty == 200.0
    assert cash == INITIAL_CAPITAL - 10_000.0
    # Equity is continuous through the split: 200 * 50 == 100 * 100.
    assert [equity for _, equity in curve] == [INITIAL_CAPITAL] * 10
    assert last_close == 50.0


def test_dividend_credits_cash_on_pay_date_from_ex_date_quantity() -> None:
    bars = _daily_bars([100.0] * 10)
    buy = {"ts": START, "is_buy": True, "qty": 100.0, "px": 100.0, "commission": 0.0}
    dividend = {"ex_date": _day(3), "kind": "dividend", "value": 0.5, "pay_date": _day(6)}

    curve, cash, _, _, applied = _equity_curve(
        INITIAL_CAPITAL,
        bars,
        [buy],
        START,
        [dividend],
    )
    assert applied
    assert cash == INITIAL_CAPITAL - 10_000.0 + 50.0
    equities = [equity for _, equity in curve]
    # Entitled at ex-date, paid at pay date: the credit appears only from day 6.
    assert equities[:6] == [INITIAL_CAPITAL] * 6
    assert equities[6:] == [INITIAL_CAPITAL + 50.0] * 4


def test_dividend_pay_date_beyond_series_still_credits_ending_cash() -> None:
    bars = _daily_bars([100.0] * 5)
    buy = {"ts": START, "is_buy": True, "qty": 100.0, "px": 100.0, "commission": 0.0}
    dividend = {"ex_date": _day(2), "kind": "dividend", "value": 1.0, "pay_date": _day(30)}

    _, cash, _, _, applied = _equity_curve(INITIAL_CAPITAL, bars, [buy], START, [dividend])
    assert applied
    assert cash == INITIAL_CAPITAL - 10_000.0 + 100.0


def test_flat_position_sees_no_ledger_effects() -> None:
    bars = _daily_bars([100.0] * 5 + [50.0] * 5)
    actions = [
        {"ex_date": _day(5), "kind": "split", "value": 2.0, "pay_date": None},
        {"ex_date": _day(5), "kind": "dividend", "value": 1.0, "pay_date": None},
    ]
    _, cash, qty, _, applied = _equity_curve(INITIAL_CAPITAL, bars, [], START, actions)
    assert not applied
    assert cash == INITIAL_CAPITAL
    assert qty == 0.0


def test_parse_corporate_actions_validates_shape() -> None:
    parsed = _parse_corporate_actions(
        [
            {"exDate": "2026-06-10", "kind": "split", "factor": 10.0},
            {"exDate": "2026-05-12", "kind": "dividend", "amount": 0.26, "payDate": "2026-05-15"},
        ],
    )
    assert [action["ex_date"] for action in parsed] == [date(2026, 5, 12), date(2026, 6, 10)]
    assert parsed[1]["value"] == 10.0
    assert parsed[0]["pay_date"] == date(2026, 5, 15)

    with pytest.raises(ValueError, match="corporateActions"):
        _parse_corporate_actions([{"exDate": "2026-06-10", "kind": "merger"}])
    with pytest.raises(ValueError, match="corporateActions"):
        _parse_corporate_actions([{"exDate": "not-a-date", "kind": "split", "factor": 2.0}])


# --- E2 acceptance: hold through a split and a dividend -----------------------

SPLIT_BAR = 15
BAR_COUNT = 30

PLAN = {
    "runtimeContractVersion": "quantchat_strategy_intent_v4",
    "features": [{"id": "sma_5", "kind": "sma", "field": "close", "period": 5}],
    "startupActions": [{"kind": "buy_fixed_notional", "amount": 10_000}],
    "rules": [
        {
            # A split gap on a raw feed makes close crash below its own SMA and
            # fires this exit; the adjusted feed must not.
            "id": "panic_exit",
            "trigger": {"kind": "bar_close"},
            "conditions": [
                {
                    "kind": "compare",
                    "op": "<",
                    "left": {"kind": "bar_field", "field": "close"},
                    "right": {"kind": "feature", "featureId": "sma_5"},
                },
            ],
            "actions": [{"kind": "exit_position"}],
        },
    ],
    "risk": {},
}


def _plan_config(corporate_actions: list[dict] | None) -> dict:
    closes = [100.0] * SPLIT_BAR + [50.0] * (BAR_COUNT - SPLIT_BAR)
    runtime_bindings: dict = {
        "instrumentSymbol": "TSLA",
        "timeframe": "1d",
        "initialCapital": str(INITIAL_CAPITAL),
        "baseCurrency": "USD",
        "startTime": START.isoformat(),
        "endTime": (START + timedelta(days=BAR_COUNT)).isoformat(),
    }
    if corporate_actions is not None:
        runtime_bindings["corporateActions"] = corporate_actions
    return {
        "compiledPlan": PLAN,
        "effectiveParameters": {},
        "runtimeBindings": runtime_bindings,
        "simulationSettings": {"feesBps": 0, "slippageBps": 0},
        "bars": _daily_bars(closes),
    }


def test_hold_through_split_and_dividend_is_total_return_correct() -> None:
    actions = [
        {
            "exDate": _day(8).isoformat(),
            "kind": "dividend",
            "amount": 1.0,
            "payDate": _day(10).isoformat(),
        },
        {"exDate": _day(SPLIT_BAR).isoformat(), "kind": "split", "factor": 2.0},
    ]

    with_ca = run_backtest_plan(_plan_config(actions))["summaryMetrics"]
    without_ca = run_backtest_plan(_plan_config(None))["summaryMetrics"]

    # Adjusted feed: the split never fires the exit — the buy is the only fill.
    assert with_ca["totalTrades"] == 1
    # Raw feed: the split gap crashes close below its SMA and the exit fires.
    assert without_ca["totalTrades"] == 2

    # Total-return correctness: a flat (split-adjusted) price path with ~100
    # shares held means ending equity is the initial capital plus the ~$100
    # dividend, less only the one-tick spread the buy crossed.
    delta = with_ca["endingEquity"] - INITIAL_CAPITAL
    assert 50.0 < delta < 150.0

    # The raw run sold ~100 shares (bought near 100) at ~50: roughly half the
    # position notional is gone. The dual-series run must not show that hole.
    assert without_ca["endingEquity"] < INITIAL_CAPITAL - 4_000.0
    assert with_ca["endingEquity"] > without_ca["endingEquity"] + 4_000.0
