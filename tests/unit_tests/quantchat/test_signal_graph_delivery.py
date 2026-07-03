"""
Composable-signals Phase 4 chunk 2: execution-graph delivery.

Covers the native signal-graph path: a `run_backtest_plan` config carrying
`runtimeBindings.signalGraph` (the backend's flattened, fully-resolved
`signal_graph_v1` graph for a strategy composing `module_ref` features)
builds the `SignalEngine` directly from that graph JSON
(`SignalEngine(graph_json, params)`) instead of translating
`compiledPlan.features` via `SignalEngine.from_legacy_features`, and a rule
reading the graph's named output via `{"kind": "feature", "featureId": ...}`
sees the same values either way.

This is the acceptance test the task calls out explicitly: "a run_backtest_plan
config carrying a signalGraph (hand-built simple graph: sma over close) executes
and its rules read the output."

Requires the compiled nautilus core and the signal_engine wheel.
"""

from datetime import UTC
from datetime import datetime
from datetime import timedelta

import pytest


pytest.importorskip("nautilus_trader.backtest.engine", reason="requires built nautilus core")

from nautilus_trader.quantchat.run_backtest import run_backtest_plan


START = datetime(2026, 1, 5, tzinfo=UTC)
INITIAL_CAPITAL = 100_000.0

# A hand-built signal_graph_v1 graph: sma(close, 5), named output "sma_5" —
# byte-identical to what `signal_engine::stdlib::sma` over a `Node::Bar{Close}`
# produces (verified against the Rust `Graph`'s own canonical serialization).
SMA_5_GRAPH = {
    "schema_version": "signal_graph_v1",
    "nodes": [
        {"kind": "bar", "field": "close"},
        {"kind": "roll", "agg": "mean", "input": 0, "period": 5},
    ],
    "outputs": {"sma_5": 1},
}

# Same rule shape as test_corporate_actions.py's PLAN: exit when close crosses
# below the sma_5 feature. compiledPlan.features carries an unrelated price
# feature only (the engine rejects an empty graph), never an "sma_5" feature —
# so this condition can only ever read a real value when the engine was built
# from `runtimeBindings.signalGraph`, proving the native path, not a legacy
# feature translation, produced "sma_5".
PLAN = {
    "runtimeContractVersion": "quantchat_strategy_intent_v7",
    "features": [{"id": "price_close", "kind": "price", "field": "close"}],
    "startupActions": [{"kind": "buy_fixed_notional", "amount": 10_000}],
    "rules": [
        {
            "id": "exit_below_sma",
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


def _daily_bars(closes: list[float]) -> list[dict]:
    bars = []
    for index, close in enumerate(closes):
        ts = START + timedelta(days=index)
        bars.append(
            {
                "timestamp": ts.isoformat(),
                "open": close,
                "high": close + 1,
                "low": close - 1,
                "close": close,
                "volume": 1_000.0,
            },
        )
    return bars


def _run(closes: list[float], signal_graph: dict | None) -> dict:
    bars = _daily_bars(closes)
    runtime_bindings = {
        "instrumentSymbol": "TSLA",
        "timeframe": "1d",
        "initialCapital": str(INITIAL_CAPITAL),
        "baseCurrency": "USD",
        "startTime": START.isoformat(),
        "endTime": (START + timedelta(days=len(closes))).isoformat(),
    }
    if signal_graph is not None:
        runtime_bindings["signalGraph"] = signal_graph
    config = {
        "compiledPlan": PLAN,
        "effectiveParameters": {},
        "runtimeBindings": runtime_bindings,
        "simulationSettings": {"feesBps": 0, "slippageBps": 0},
        "bars": bars,
    }
    return run_backtest_plan(config)


class TestSignalGraphDelivery:
    def test_signal_graph_executes_and_rule_reads_its_output(self) -> None:
        # Flat at 100 for 5 bars (SMA warms up with no cross), then a sharp
        # drop to 50: close crosses below sma_5 and the exit rule must fire —
        # provable only if the engine actually evaluated `sma_5` from
        # `signalGraph`, since compiledPlan.features is empty.
        closes = [100.0] * 5 + [50.0] * 3
        result = _run(closes, SMA_5_GRAPH)

        orders = result["artifactManifest"]["orders"]
        assert len(orders) == 2, "expected an entry buy plus an sma-cross exit sell"
        assert orders[0]["side"] == "BUY"
        exit_orders = [o for o in orders if o["side"] == "SELL"]
        assert len(exit_orders) == 1
        assert exit_orders[0]["status"] == "FILLED"

    def test_missing_signal_graph_with_unrelated_features_never_fires(self) -> None:
        # Sanity control: the identical bar series and plan, but with no
        # signalGraph — "sma_5" is genuinely unknown (compiledPlan.features
        # only declares "price_close"), `_feature_value` returns None -> the
        # compare condition never reads a real value -> the exit never fires.
        # This is what proves the first test's fill is caused by signalGraph,
        # not some other path.
        closes = [100.0] * 5 + [50.0] * 3
        result = _run(closes, None)

        orders = result["artifactManifest"]["orders"]
        sell_orders = [o for o in orders if o["side"] == "SELL"]
        assert sell_orders == []
