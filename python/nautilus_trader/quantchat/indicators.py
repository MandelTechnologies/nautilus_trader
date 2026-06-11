"""
Incremental indicator state for compiled-plan features.

One state instance per plan feature. States are fed every bar — warmup and live
alike, through the strategy's single append path — and answer value queries at
bar offsets 0 (current) and 1 (previous) for cross conditions. All math is
windowed or recursive: per-bar cost is bounded by the indicator period and never
depends on how long the strategy has been running.

Semantics follow the de-facto charting standard (TradingView):

- ``sma``: arithmetic mean of the last ``period`` values.
- ``ema``: seeded with the SMA of the first ``period`` values, then
  ``ema = alpha * x + (1 - alpha) * ema`` with ``alpha = 2 / (period + 1)``.
- ``rsi``: Wilder's RSI on a 0-100 scale. Average gain and loss are Wilder
  moving averages — seeded with the simple average of the first ``period``
  changes, then ``avg = (avg * (period - 1) + x) / period`` — and
  ``RSI = 100 - 100 / (1 + gain/loss)``, or 100 while the average loss is zero.
- ``bollinger``: SMA of the field plus/minus ``stdDev`` population standard
  deviations of the window.
- ``rolling_high`` / ``rolling_low``: max/min of the last ``period`` values.

EMA and RSI are recursive (infinite-impulse): their values depend on where the
series was seeded. The compiler sizes ``warmupBars`` so that influence from
before the seed decays below ~1e-4 (5x period for EMA, 10x period for RSI),
making a freshly seeded strategy agree with a long-running one to well below
decision thresholds — and making any backtest over the same window reproduce a
live bot booted at its start.

"""

from __future__ import annotations

from collections import deque
from math import fsum
from math import isfinite
from math import sqrt
from typing import Any


# Bounds memory and per-bar work; sized for the longest conventional lookback
# (a one-year daily window).
MAX_INDICATOR_PERIOD = 365


def _validate_period(raw: float) -> int:
    period = int(raw)
    if period != raw or not 1 <= period <= MAX_INDICATOR_PERIOD:
        raise ValueError(
            f"Indicator period must be a whole number between 1 and "
            f"{MAX_INDICATOR_PERIOD}; got {raw}",
        )
    return period


class FeatureState:
    """
    Base class: extracts the configured bar field, delegates the math to
    ``_compute``, and keeps the last two computed values so conditions can read
    offsets 0 and 1 (for crosses).
    """

    def __init__(self, field: str) -> None:
        self.field = field
        self._ring: deque[float | None] = deque(maxlen=2)

    def update(self, record: dict[str, Any]) -> None:
        value = float(record[self.field])
        if not isfinite(value):
            raise ValueError(f"Bar field {self.field!r} is not finite: {value}")
        self._ring.append(self._compute(value))

    def value_at(self, offset: int) -> float | None:
        if offset < 0 or offset >= len(self._ring):
            return None
        return self._ring[-1 - offset]

    def _compute(self, value: float) -> float | None:
        raise NotImplementedError


class _Sma(FeatureState):
    def __init__(self, field: str, period: int) -> None:
        super().__init__(field)
        self._period = period
        self._window: deque[float] = deque(maxlen=period)

    def _compute(self, value: float) -> float | None:
        self._window.append(value)
        if len(self._window) < self._period:
            return None
        return fsum(self._window) / self._period


class _Ema(FeatureState):
    def __init__(self, field: str, period: int) -> None:
        super().__init__(field)
        self._period = period
        self._alpha = 2.0 / (period + 1.0)
        self._seed: list[float] = []
        self._value: float | None = None

    def _compute(self, value: float) -> float | None:
        if self._value is None:
            self._seed.append(value)
            if len(self._seed) < self._period:
                return None
            self._value = fsum(self._seed) / self._period
            self._seed.clear()
        else:
            self._value = self._alpha * value + (1.0 - self._alpha) * self._value
        return self._value


class _WilderRsi(FeatureState):
    def __init__(self, field: str, period: int) -> None:
        super().__init__(field)
        self._period = period
        self._prev: float | None = None
        self._seed_gains: list[float] = []
        self._seed_losses: list[float] = []
        self._avg_gain: float | None = None
        self._avg_loss: float | None = None

    def _compute(self, value: float) -> float | None:
        if self._prev is None:
            self._prev = value
            return None
        change = value - self._prev
        self._prev = value
        gain = max(change, 0.0)
        loss = max(-change, 0.0)
        if self._avg_gain is None or self._avg_loss is None:
            self._seed_gains.append(gain)
            self._seed_losses.append(loss)
            if len(self._seed_gains) < self._period:
                return None
            self._avg_gain = fsum(self._seed_gains) / self._period
            self._avg_loss = fsum(self._seed_losses) / self._period
            self._seed_gains.clear()
            self._seed_losses.clear()
        else:
            n = self._period
            self._avg_gain = (self._avg_gain * (n - 1) + gain) / n
            self._avg_loss = (self._avg_loss * (n - 1) + loss) / n
        if self._avg_loss == 0.0:
            return 100.0
        rs = self._avg_gain / self._avg_loss
        return 100.0 - 100.0 / (1.0 + rs)


class _Bollinger(FeatureState):
    def __init__(self, field: str, period: int, std_dev: float, band: str) -> None:
        super().__init__(field)
        if not isfinite(std_dev) or std_dev <= 0:
            raise ValueError(f"Bollinger stdDev must be positive and finite; got {std_dev}")
        if band not in {"upper", "middle", "lower"}:
            raise ValueError(f"Bollinger band must be upper, middle, or lower; got {band!r}")
        self._period = period
        self._k = std_dev
        self._band = band
        self._window: deque[float] = deque(maxlen=period)

    def _compute(self, value: float) -> float | None:
        self._window.append(value)
        if len(self._window) < self._period:
            return None
        middle = fsum(self._window) / self._period
        if self._band == "middle":
            return middle
        # Population sigma via a two-pass fsum: numerically exact and C-speed
        # (statistics.pstdev would be exact too, but via slow Fraction math).
        sigma = sqrt(fsum((x - middle) ** 2 for x in self._window) / self._period)
        return middle + self._k * sigma if self._band == "upper" else middle - self._k * sigma


class _RollingExtreme(FeatureState):
    def __init__(self, field: str, period: int, is_high: bool) -> None:
        super().__init__(field)
        self._period = period
        self._is_high = is_high
        self._window: deque[float] = deque(maxlen=period)

    def _compute(self, value: float) -> float | None:
        self._window.append(value)
        if len(self._window) < self._period:
            return None
        return max(self._window) if self._is_high else min(self._window)


def build_feature_state(
    kind: str,
    field: str,
    period: float,
    std_dev: float,
    band: str,
) -> FeatureState:
    """
    Build the incremental state for an indicator feature kind.

    Raises ``ValueError`` for unsupported kinds or out-of-range settings so a bad
    plan fails loudly at boot, never silently mid-run.

    """
    validated = _validate_period(period)
    if kind == "sma":
        return _Sma(field, validated)
    if kind == "ema":
        return _Ema(field, validated)
    if kind == "rsi":
        return _WilderRsi(field, validated)
    if kind == "bollinger":
        return _Bollinger(field, validated, std_dev, band)
    if kind == "rolling_high":
        return _RollingExtreme(field, validated, is_high=True)
    if kind == "rolling_low":
        return _RollingExtreme(field, validated, is_high=False)
    raise ValueError(f"Unsupported indicator feature kind: {kind!r}")
