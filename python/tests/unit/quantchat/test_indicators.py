"""
Reference and property tests for the incremental indicator states.

The module under test is pure stdlib, so it is loaded by file path and these tests run
without the compiled nautilus core.

"""

import importlib.util
from pathlib import Path
import random
import statistics
import time

import pytest


_INDICATORS_PATH = (
    Path(__file__).resolve().parents[3] / "nautilus_trader" / "quantchat" / "indicators.py"
)
_SPEC = importlib.util.spec_from_file_location("indicators_under_test", _INDICATORS_PATH)
assert _SPEC is not None and _SPEC.loader is not None
indicators = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(indicators)


def _state(kind: str, period: float, std_dev: float = 2.0, band: str = "middle"):
    return indicators.build_feature_state(
        kind=kind,
        field="close",
        period=period,
        std_dev=std_dev,
        band=band,
    )


def _feed(state, values):
    out = []
    for value in values:
        state.update({"close": float(value)})
        out.append(state.value_at(0))
    return out


def test_sma_warms_after_period_then_slides() -> None:
    assert _feed(_state("sma", 3), [1, 2, 3, 4]) == [None, None, 2.0, 3.0]


def test_ema_seeds_with_sma_then_recurses() -> None:
    # period 3 -> alpha 0.5; seed = mean(1,2,3) = 2; then 0.5*4+0.5*2, 0.5*5+0.5*3.
    assert _feed(_state("ema", 3), [1, 2, 3, 4, 5]) == [None, None, 2.0, 3.0, 4.0]


def test_rsi_is_wilders_on_0_100_scale() -> None:
    # period 3 over 1,2,3,4,3,2: first value only sets prev; the seed needs three
    # changes (+1,+1,+1) -> avg gain 1, avg loss 0 -> 100. Then Wilder smoothing:
    # -1 -> gain 2/3, loss 1/3 -> RS 2 -> 66.67; -1 -> gain 4/9, loss 5/9 -> 44.44.
    values = _feed(_state("rsi", 3), [1, 2, 3, 4, 3, 2])
    assert values[:3] == [None, None, None]
    assert values[3] == 100.0
    assert values[4] == pytest.approx(100.0 - 100.0 / (1.0 + 2.0))
    assert values[5] == pytest.approx(100.0 - 100.0 / (1.0 + 0.8))


def test_bollinger_bands_use_population_sigma_of_field() -> None:
    window = [1.0, 2.0, 3.0]
    middle = _feed(_state("bollinger", 3, band="middle"), window)[-1]
    upper = _feed(_state("bollinger", 3, std_dev=2.0, band="upper"), window)[-1]
    lower = _feed(_state("bollinger", 3, std_dev=2.0, band="lower"), window)[-1]
    sigma = statistics.pstdev(window)
    assert middle == pytest.approx(2.0)
    assert upper == pytest.approx(2.0 + 2.0 * sigma)
    assert lower == pytest.approx(2.0 - 2.0 * sigma)


def test_rolling_extremes_expire_old_values() -> None:
    assert _feed(_state("rolling_high", 2), [1, 3, 2, 5]) == [None, 3.0, 3.0, 5.0]
    assert _feed(_state("rolling_low", 2), [1, 3, 2, 5]) == [None, 1.0, 2.0, 2.0]


def test_value_ring_serves_cross_offsets() -> None:
    state = _state("sma", 2)
    state.update({"close": 1.0})
    assert state.value_at(0) is None
    assert state.value_at(1) is None  # Out of range on the first bar.
    state.update({"close": 3.0})
    state.update({"close": 5.0})
    assert state.value_at(0) == 4.0
    assert state.value_at(1) == 2.0
    assert state.value_at(2) is None  # Beyond the ring.


@pytest.mark.parametrize("bad_period", [0, 366, 2.5, -3])
def test_out_of_range_periods_fail_loudly(bad_period) -> None:
    with pytest.raises(ValueError, match="period"):
        _state("sma", bad_period)


def test_unsupported_settings_fail_loudly() -> None:
    with pytest.raises(ValueError, match="kind"):
        _state("macd", 14)
    with pytest.raises(ValueError, match="band"):
        _state("bollinger", 14, band="median")
    with pytest.raises(ValueError, match="stdDev"):
        _state("bollinger", 14, std_dev=0.0)
    with pytest.raises(ValueError, match="finite"):
        _state("sma", 2).update({"close": float("nan")})


def _random_walk(count: int, seed: int = 42) -> list[float]:
    rng = random.Random(seed)  # noqa: S311 (deterministic test data, not crypto)
    price = 100.0
    series = []
    for _ in range(count):
        price = max(1.0, price * (1.0 + rng.gauss(0.0, 0.002)))
        series.append(price)
    return series


def test_window_indicators_are_pure_functions_of_the_last_period_bars() -> None:
    # FIR indicators seeded with only the last `period` values must match a
    # state that saw the entire history — this is what makes restarts exact.
    series = _random_walk(2_000)
    for kind in ("sma", "bollinger", "rolling_high", "rolling_low"):
        full = _state(kind, 50, band="upper")
        _feed(full, series)
        restarted = _state(kind, 50, band="upper")
        _feed(restarted, series[-50:])
        assert restarted.value_at(0) == pytest.approx(full.value_at(0), rel=1e-12)


def test_recursive_indicators_converge_within_compiler_warmup() -> None:
    # EMA and RSI are infinite-impulse: the compiler sizes warmup at 5x / 10x
    # period so a freshly seeded state agrees with a long-running one.
    series = _random_walk(20_000)
    for kind, period, multiple in (("ema", 20, 5), ("rsi", 14, 10)):
        full = _state(kind, period)
        _feed(full, series)
        restarted = _state(kind, period)
        _feed(restarted, series[-period * multiple :])
        assert restarted.value_at(0) == pytest.approx(full.value_at(0), rel=1e-3)


def test_per_bar_cost_does_not_grow_with_history() -> None:
    # The old EMA implementation rescanned the full history every bar (O(n^2));
    # 100k updates would take minutes. Generous bound, robust on slow CI.
    states = [
        _state("ema", 20),
        _state("rsi", 14),
        _state("sma", 200),
        _state("bollinger", 200, band="upper"),
        _state("rolling_high", 365),
    ]
    series = _random_walk(100_000)
    started = time.perf_counter()
    for value in series:
        record = {"close": value}
        for state in states:
            state.update(record)
    elapsed = time.perf_counter() - started
    assert elapsed < 30.0, f"indicator updates took {elapsed:.1f}s for 100k bars"
