"""
Phase 3(c)+(d): first-class order-type execution and sizing-expression evaluation
(composable-signals Sec6.2/Sec6.3).

Covers:
  - `_sizing_value`'s expression tree (legacy passthrough, tagged kinds, the Kelly
    shape), exercised end to end through `run_backtest_plan` since the evaluator
    reads live account/position state that only exists inside a running strategy.
  - Acceptance: a stop order breached mid-run fills at the stop trigger, not the
    next bar's close, and `artifactManifest` discloses orders + the intrabar fill
    policy.
  - Acceptance: a single bar spanning both a bracket's stop and target resolves via
    nautilus's real fixed O->H->L->C ordering (documented, not the naive "stop
    always first" claim) with the OCO sibling canceled.
  - Runtime-contract rejection: a v7 plan fed to a frozen v6-only supported set
    raises before any order logic runs.

Requires the compiled nautilus core and the signal_engine wheel.

"""

from datetime import UTC
from datetime import datetime
from datetime import timedelta

import pytest


pytest.importorskip("nautilus_trader.backtest.engine", reason="requires built nautilus core")

from nautilus_trader.quantchat.intent_strategy import _SUPPORTED_RUNTIME_CONTRACTS
from nautilus_trader.quantchat.intent_strategy import _validate_runtime_contract
from nautilus_trader.quantchat.run_backtest import run_backtest_plan


START = datetime(2026, 1, 5, tzinfo=UTC)
INITIAL_CAPITAL = 100_000.0

# Every plan needs at least one feature: the signal engine rejects an empty graph.
_PRICE_FEATURE = [{"id": "price_close", "kind": "price", "field": "close"}]


def _daily_bars(rows: list[tuple[float, float, float, float]]) -> list[dict]:
    bars = []
    for index, (open_, high, low, close) in enumerate(rows):
        ts = START + timedelta(days=index)
        bars.append(
            {
                "timestamp": ts.isoformat(),
                "open": open_,
                "high": high,
                "low": low,
                "close": close,
                "volume": 1_000.0,
            },
        )
    return bars


def _run(plan: dict, rows: list[tuple[float, float, float, float]]) -> dict:
    bars = _daily_bars(rows)
    config = {
        "compiledPlan": plan,
        "effectiveParameters": {},
        "runtimeBindings": {
            "instrumentSymbol": "TSLA",
            "timeframe": "1d",
            "initialCapital": str(INITIAL_CAPITAL),
            "baseCurrency": "USD",
            "startTime": START.isoformat(),
            "endTime": (START + timedelta(days=len(rows))).isoformat(),
        },
        "simulationSettings": {"feesBps": 0, "slippageBps": 0},
        "bars": bars,
    }
    return run_backtest_plan(config)


def _orders_by_type(result: dict) -> dict[str, list[dict]]:
    by_type: dict[str, list[dict]] = {}
    for order in result["artifactManifest"]["orders"]:
        by_type.setdefault(order["orderType"], []).append(order)
    return by_type


# --- (a) _sizing_value, exercised end to end -----------------------------------


class TestSizingValue:
    def test_legacy_bare_number_and_param_pass_through(self) -> None:
        # A pre-v7 plan's bare-number `amount` must still evaluate unchanged: this
        # is the untagged-superset guarantee `SizingExprV1` makes over `NumberExprV1`.
        plan = {
            "runtimeContractVersion": "quantchat_strategy_intent_v7",
            "features": _PRICE_FEATURE,
            "startupActions": [{"kind": "buy_fixed_notional", "amount": 10_000}],
            "rules": [],
            "risk": {},
        }
        result = _run(plan, [(100, 100, 100, 100), (100, 101, 99, 100)])
        orders = result["artifactManifest"]["orders"]
        assert len(orders) == 1
        assert orders[0]["status"] == "FILLED"
        # $10,000 at a $100 fill is 100 shares.
        assert orders[0]["quantity"] == "100"

    def test_account_field_equity_scales_notional(self) -> None:
        # amount = account.equity * 0.1 against $100k starting equity -> $10k notional.
        plan = {
            "runtimeContractVersion": "quantchat_strategy_intent_v7",
            "features": _PRICE_FEATURE,
            "startupActions": [
                {
                    "kind": "buy_fixed_notional",
                    "amount": {
                        "kind": "binary_op",
                        "op": "mul",
                        "left": {"kind": "account_field", "field": "account.equity"},
                        "right": {"kind": "constant", "value": 0.1},
                    },
                },
            ],
            "rules": [],
            "risk": {},
        }
        result = _run(plan, [(100, 100, 100, 100), (100, 101, 99, 100)])
        orders = result["artifactManifest"]["orders"]
        assert len(orders) == 1
        assert orders[0]["quantity"] == "100"

    def test_kelly_shape_clamp_min_times_account_equity(self) -> None:
        # mul(clamp(min, param(kelly_fraction), param(max_kelly_weight)), account.equity)
        # kelly_fraction=0.25 clamped by max_kelly_weight=0.10 -> 10% of $100k -> $10k.
        plan = {
            "runtimeContractVersion": "quantchat_strategy_intent_v7",
            "features": _PRICE_FEATURE,
            "startupActions": [
                {
                    "kind": "buy_fixed_notional",
                    "amount": {
                        "kind": "binary_op",
                        "op": "mul",
                        "left": {
                            "kind": "clamp",
                            "op": "min",
                            "expr": {"kind": "param", "name": "kelly_fraction"},
                            "bound": {"kind": "param", "name": "max_kelly_weight"},
                        },
                        "right": {"kind": "account_field", "field": "account.equity"},
                    },
                },
            ],
            "rules": [],
            "risk": {},
        }
        config = {
            "compiledPlan": plan,
            "effectiveParameters": {"kelly_fraction": 0.25, "max_kelly_weight": 0.10},
            "runtimeBindings": {
                "instrumentSymbol": "TSLA",
                "timeframe": "1d",
                "initialCapital": str(INITIAL_CAPITAL),
                "baseCurrency": "USD",
                "startTime": START.isoformat(),
                "endTime": (START + timedelta(days=2)).isoformat(),
            },
            "simulationSettings": {"feesBps": 0, "slippageBps": 0},
            "bars": _daily_bars([(100, 100, 100, 100), (100, 101, 99, 100)]),
        }
        result = run_backtest_plan(config)
        orders = result["artifactManifest"]["orders"]
        assert len(orders) == 1
        # $10k / $100 = 100 shares: the clamp bound (0.10), not the unclamped
        # Kelly fraction (0.25), governed sizing.
        assert orders[0]["quantity"] == "100"

    def test_div_by_zero_evaluates_to_zero_not_an_error(self) -> None:
        # position.quantity is 0 while flat, so amount = account.equity / position.quantity
        # must resolve to 0.0 (no order), not raise.
        plan = {
            "runtimeContractVersion": "quantchat_strategy_intent_v7",
            "features": _PRICE_FEATURE,
            "startupActions": [
                {
                    "kind": "buy_fixed_notional",
                    "amount": {
                        "kind": "binary_op",
                        "op": "div",
                        "left": {"kind": "account_field", "field": "account.equity"},
                        "right": {"kind": "account_field", "field": "position.quantity"},
                    },
                },
            ],
            "rules": [],
            "risk": {},
        }
        result = _run(plan, [(100, 100, 100, 100), (100, 101, 99, 100)])
        assert result["artifactManifest"]["orders"] == []
        assert result["summaryMetrics"]["totalTrades"] == 0


# --- (b) stop order fills at the stop price, not next-bar close ----------------


class TestStopOrderAcceptance:
    def test_stop_5pct_below_avg_cost_fills_at_stop_not_next_close(self) -> None:
        # Entry at 100 (market). stopPrice = position.avgCost * 0.95 = 95. Bar 3
        # dips to a low of 90 (well past the stop) with a close of 95.50 — if the
        # runtime naively filled at the next bar's close instead of the stop
        # trigger, the fill price would be wrong.
        rows = [
            (100, 100, 100, 100),
            (100, 101, 99, 100),
            (100, 101, 99, 100),
            (100, 100, 90, 95.50),
            (95.50, 96, 94, 95.50),
        ]
        # A bare `orderType: "stop"` startup action has no prior position to size
        # the stop off (position.avgCost is 0 while flat), so this uses a
        # `bracket` entry: the market entry fills first, then the stop's price
        # is evaluated against the realized avgCost (PINNED semantic) and
        # submitted as a resting STOP_MARKET exit — the case this test is
        # actually about.
        plan = {
            "runtimeContractVersion": "quantchat_strategy_intent_v7",
            "features": _PRICE_FEATURE,
            "startupActions": [
                {
                    "kind": "buy_fixed_notional",
                    "amount": 10_000,
                    "orderType": "bracket",
                    "stopPrice": {
                        "kind": "binary_op",
                        "op": "mul",
                        "left": {"kind": "account_field", "field": "position.avgCost"},
                        "right": {"kind": "constant", "value": 0.95},
                    },
                },
            ],
            "rules": [],
            "risk": {},
        }

        result = _run(plan, rows)
        orders = result["artifactManifest"]["orders"]
        by_type = _orders_by_type(result)

        assert "MARKET" in by_type
        assert by_type["MARKET"][0]["status"] == "FILLED"
        assert by_type["MARKET"][0]["avg_px"] == 100.0

        assert "STOP_MARKET" in by_type
        stop_order = by_type["STOP_MARKET"][0]
        assert stop_order["status"] == "FILLED"
        # Filled at (or acceptably near) the stop trigger price, not the bar's
        # eventual close of 95.50.
        assert stop_order["avg_px"] == pytest.approx(95.0, abs=0.01)

        assert result["summaryMetrics"]["totalTrades"] == 2
        assert orders  # artifactManifest.orders is populated, not the old []
        assert "intrabarFillPolicy" in result["artifactManifest"]["simulationSettingsApplied"]


# --- (c) one bar spans both bracket levels: document actual nautilus behavior --


class TestBracketSpansBothLevelsInOneBar:
    def test_long_bracket_one_bar_touches_both_levels(self) -> None:
        """
        A long bracket's stop sits below entry and its target sits above.

        When a
        single bar's `high` crosses the target AND its `low` crosses the stop,
        nautilus's default (`bar_adaptive_high_low_ordering=False`) synthesizes
        trade ticks in a FIXED Open->High->Low->Close order per bar. For a LONG
        bracket that means the favorable/target touch (High) is processed BEFORE
        the adverse/stop touch (Low) — the OPPOSITE of a naive "adverse fill
        assumed first" claim. This test asserts nautilus's actual behavior (target
        fills, stop is OCO-canceled) and documents it, rather than asserting the
        spec's illustrative "stop always first" text, which does not hold for
        longs under the fixed ordering (see run_backtest.py's disclosed
        `intrabarFillPolicy`: "conservative for short brackets and optimistic for
        long brackets when a single bar spans both levels").

        """
        rows = [
            (100, 100, 100, 100),
            (100, 101, 99, 100),
            (100, 101, 99, 100),
            (100, 110, 90, 100),  # spans both stop (95) and target (105) in one bar
            (100, 101, 99, 100),
        ]
        plan = {
            "runtimeContractVersion": "quantchat_strategy_intent_v7",
            "features": _PRICE_FEATURE,
            "startupActions": [
                {
                    "kind": "buy_fixed_notional",
                    "amount": 10_000,
                    "orderType": "bracket",
                    "stopPrice": {
                        "kind": "binary_op",
                        "op": "mul",
                        "left": {"kind": "account_field", "field": "position.avgCost"},
                        "right": {"kind": "constant", "value": 0.95},
                    },
                    "targetPrice": {
                        "kind": "binary_op",
                        "op": "mul",
                        "left": {"kind": "account_field", "field": "position.avgCost"},
                        "right": {"kind": "constant", "value": 1.05},
                    },
                },
            ],
            "rules": [],
            "risk": {},
        }
        result = _run(plan, rows)
        by_type = _orders_by_type(result)

        assert by_type["MARKET"][0]["status"] == "FILLED"

        # Exactly one of stop/target fills; the OCO sibling is canceled, never
        # left dangling or double-filled.
        stop_order = by_type["STOP_MARKET"][0]
        target_order = by_type["LIMIT"][0]
        filled = [o for o in (stop_order, target_order) if o["status"] == "FILLED"]
        canceled = [o for o in (stop_order, target_order) if o["status"] == "CANCELED"]
        assert len(filled) == 1
        assert len(canceled) == 1

        # Documented actual behavior: nautilus's fixed O->H->L->C ordering
        # processes the bar's High (target touch) before its Low (stop touch)
        # for a long position, so the TARGET fills and the STOP is the OCO
        # cancellation casualty — the opposite of "stop always first".
        assert filled[0] is target_order
        assert canceled[0] is stop_order
        assert target_order["avg_px"] == pytest.approx(105.0, abs=0.01)

        assert result["summaryMetrics"]["totalTrades"] == 2


# --- (d) runtime-contract rejection ---------------------------------------------


class TestRuntimeContractRejection:
    def test_v7_plan_rejected_by_a_v6_only_runtime(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            "nautilus_trader.quantchat.intent_strategy._SUPPORTED_RUNTIME_CONTRACTS",
            {
                "quantchat_strategy_intent_v4",
                "quantchat_strategy_intent_v5",
                "quantchat_strategy_intent_v6",
            },
        )
        with pytest.raises(ValueError, match="Unsupported runtime contract"):
            _validate_runtime_contract({"runtimeContractVersion": "quantchat_strategy_intent_v7"})

    def test_v7_is_supported_today(self) -> None:
        assert "quantchat_strategy_intent_v7" in _SUPPORTED_RUNTIME_CONTRACTS
        _validate_runtime_contract({"runtimeContractVersion": "quantchat_strategy_intent_v7"})
