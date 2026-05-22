from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from decimal import Decimal
from itertools import pairwise
from math import isfinite
from operator import eq
from operator import ge
from operator import gt
from operator import le
from operator import lt
from operator import ne
from statistics import pstdev
from typing import Any
from zoneinfo import ZoneInfo
from zoneinfo import ZoneInfoNotFoundError

from nautilus_trader.common.events import TimeEvent
from nautilus_trader.config import StrategyConfig
from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import BarType
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.objects import Currency
from nautilus_trader.trading.strategy import Strategy


_COMPARE_OPERATORS = {
    "<": lt,
    "<=": le,
    ">": gt,
    ">=": ge,
    "==": eq,
    "!=": ne,
}


@dataclass(frozen=True)
class QuantChatRuntime:
    instrument_id: InstrumentId
    bar_type: BarType
    symbol: str
    timeframe: str
    base_currency: str = "USD"
    start_time: str = ""
    end_time: str = ""
    market_calendar: dict[str, Any] | None = None


class QuantChatIntentStrategyConfig(StrategyConfig, frozen=True):
    instrument_id: InstrumentId
    bar_type: BarType
    symbol: str
    timeframe: str
    base_currency: str
    start_time: str
    end_time: str
    market_calendar: dict[str, Any]
    compiled_plan: dict[str, Any]
    parameters: dict[str, Any]


def build_intent_strategy(
    runtime: QuantChatRuntime,
    compiled_plan: dict[str, Any],
    parameters: dict[str, Any],
) -> Strategy:
    return QuantChatIntentStrategy(
        QuantChatIntentStrategyConfig(
            instrument_id=runtime.instrument_id,
            bar_type=runtime.bar_type,
            symbol=runtime.symbol,
            timeframe=runtime.timeframe,
            base_currency=runtime.base_currency,
            start_time=runtime.start_time,
            end_time=runtime.end_time,
            market_calendar=runtime.market_calendar or {},
            compiled_plan=compiled_plan,
            parameters=parameters,
        ),
    )


class QuantChatIntentStrategy(Strategy):
    def __init__(self, config: QuantChatIntentStrategyConfig) -> None:
        super().__init__(config)
        self._bars: list[dict[str, float]] = []
        self._startup_done = False
        self._feature_cache: dict[tuple[str, int], float | None] = {}
        self._trades_today: dict[str, int] = {}
        self._last_event_ts_ns: int | None = None

    def on_start(self) -> None:
        self._validate_wall_clock_calendars()
        self.subscribe_bars(self.config.bar_type)
        self._setup_wall_clock_triggers()
        self.log.info("QuantChat intent strategy started")

    def on_bar(self, bar: Bar) -> None:
        self._bars.append(
            {
                "open": float(bar.open),
                "high": float(bar.high),
                "low": float(bar.low),
                "close": float(bar.close),
                "volume": float(bar.volume),
                "ts_event": float(bar.ts_event),
            },
        )
        self._last_event_ts_ns = None
        self._feature_cache.clear()

        if not self._in_run_window():
            return

        if not self._startup_done:
            self._startup_done = True
            for action in self._plan_list("startupActions"):
                self._execute_action(action, "startup")

        for rule in self._plan_list("rules"):
            if self._trigger_kind(rule) == "bar_close":
                self._evaluate_rule(rule, "bar_close")

    def _setup_wall_clock_triggers(self) -> None:
        after = self._run_start_utc()
        if not self.config.end_time:
            after = max(after, self.clock.utc_now().astimezone(UTC))
        after -= timedelta(microseconds=1)
        for rule in self._plan_list("rules"):
            if self._trigger_kind(rule) == "wall_clock":
                self._schedule_next_wall_clock(rule, after)

    def _on_wall_clock_rule(self, rule_id: str, event: TimeEvent) -> None:
        self._last_event_ts_ns = int(event.ts_event)
        rule = next((item for item in self._plan_list("rules") if item.get("id") == rule_id), None)
        if rule is None:
            self.log.warning(f"Wall-clock rule not found: {rule_id}")
            return
        if self._in_run_window():
            self._evaluate_rule(rule, "wall_clock")
        self._schedule_next_wall_clock(rule, self._event_datetime_utc(event))

    def _evaluate_rule(self, rule: dict[str, Any], source: str) -> None:
        rule_id = str(rule.get("id", "rule"))
        conditions = rule.get("conditions", []) or []
        result = all(self._condition(condition, offset=0) for condition in conditions)
        self._decision(rule_id, result, source)
        if not result:
            return
        for action in rule.get("actions", []) or []:
            self._execute_action(action, rule_id)

    def _trigger_kind(self, rule: dict[str, Any]) -> str:
        trigger = rule.get("trigger")
        if not isinstance(trigger, dict):
            return ""
        return str(trigger.get("kind", "")).lower()

    def _schedule_next_wall_clock(self, rule: dict[str, Any], after_utc: datetime) -> None:
        trigger = rule.get("trigger")
        if not isinstance(trigger, dict):
            return
        next_time = self._next_wall_clock_time(trigger, after_utc)
        if next_time is None or not self._within_end_time(next_time):
            return
        rule_id = str(rule.get("id", "rule"))
        alert_name = f"quantchat:{rule_id}:{int(next_time.timestamp())}"
        self.clock.set_time_alert(
            alert_name,
            next_time,
            lambda event, rule_id=rule_id: self._on_wall_clock_rule(rule_id, event),
            allow_past=False,
        )

    def _next_wall_clock_time(
        self,
        trigger: dict[str, Any],
        after_utc: datetime,
    ) -> datetime | None:
        timezone_name = str(trigger.get("timezone", "UTC"))
        try:
            tz = ZoneInfo(timezone_name)
        except ZoneInfoNotFoundError:
            self.log.error(f"Unsupported trigger timezone: {timezone_name}")
            return None

        time_text = str(trigger.get("time", ""))
        try:
            hour_text, minute_text = time_text.split(":", 1)
            hour = int(hour_text)
            minute = int(minute_text)
        except ValueError:
            self.log.error(f"Unsupported trigger time: {time_text}")
            return None

        recurrence = str(trigger.get("recurrence", "")).lower()
        after_local = after_utc.astimezone(tz)
        start_day = after_local.date()
        for day_offset in range(400):
            candidate_day = start_day + timedelta(days=day_offset)
            if not self._date_matches_trigger(candidate_day, trigger, recurrence):
                continue
            candidate_local = datetime(
                candidate_day.year,
                candidate_day.month,
                candidate_day.day,
                hour,
                minute,
                tzinfo=tz,
            )
            candidate_utc = candidate_local.astimezone(UTC)
            if not self._wall_clock_calendar_allows(candidate_day, candidate_utc, trigger):
                continue
            if candidate_utc > after_utc:
                return candidate_utc
        return None

    def _date_matches_trigger(
        self,
        candidate_day,
        trigger: dict[str, Any],
        recurrence: str,
    ) -> bool:
        if recurrence == "daily":
            return True
        if recurrence == "weekly":
            days = {str(day).lower() for day in trigger.get("daysOfWeek", []) or []}
            return candidate_day.strftime("%A").lower() in days
        if recurrence == "monthly":
            days = {int(day) for day in trigger.get("daysOfMonth", []) or []}
            return candidate_day.day in days
        return False

    def _validate_wall_clock_calendars(self) -> None:
        calendars = {
            str(rule.get("trigger", {}).get("calendar", "24/7")).upper()
            for rule in self._plan_list("rules")
            if self._trigger_kind(rule) == "wall_clock"
        }
        for calendar in calendars:
            if calendar == "24/7":
                continue
            if calendar != "XNYS":
                raise ValueError(f"Unsupported runtime market calendar: {calendar}")
            sessions = self._calendar_sessions(calendar)
            if not sessions:
                raise ValueError(f"Missing runtime market calendar sessions for {calendar}")

    def _wall_clock_calendar_allows(
        self,
        candidate_day,
        candidate_utc: datetime,
        trigger: dict[str, Any],
    ) -> bool:
        calendar = str(trigger.get("calendar", "24/7")).upper()
        if calendar == "24/7":
            return True
        if calendar != "XNYS":
            raise ValueError(f"Unsupported runtime market calendar: {calendar}")

        closed_market_policy = str(trigger.get("closedMarketPolicy", "skip")).lower()
        if closed_market_policy != "skip":
            raise ValueError(f"Unsupported closedMarketPolicy: {closed_market_policy}")

        day_key = candidate_day.isoformat()
        session = self._calendar_session(calendar, day_key)
        if session is None:
            return False

        status = str(session.get("status", "")).upper()
        if status == "MARKET_CLOSED":
            return False
        if status not in {"OPEN", "EARLY_CLOSE"}:
            raise ValueError(f"Unsupported {calendar} calendar status for {day_key}: {status}")

        opens_at = self._calendar_timestamp(session.get("opensAt"), calendar, day_key, "opensAt")
        closes_at = self._calendar_timestamp(
            session.get("closesAt"),
            calendar,
            day_key,
            "closesAt",
        )
        return opens_at <= candidate_utc <= closes_at

    def _calendar_sessions(self, calendar: str) -> dict[str, Any]:
        market_calendar = self.config.market_calendar
        if not isinstance(market_calendar, dict):
            raise ValueError("runtimeBindings.marketCalendar must be an object")
        payload = market_calendar.get(calendar)
        if not isinstance(payload, dict):
            raise ValueError(f"Missing runtime market calendar for {calendar}")
        sessions = payload.get("sessions")
        if not isinstance(sessions, dict):
            raise ValueError(
                f"runtimeBindings.marketCalendar.{calendar}.sessions must be an object",
            )
        return sessions

    def _calendar_session(self, calendar: str, day_key: str) -> dict[str, Any] | None:
        session = self._calendar_sessions(calendar).get(day_key)
        if session is None:
            return None
        if not isinstance(session, dict):
            raise ValueError(
                f"runtimeBindings.marketCalendar.{calendar}.sessions.{day_key} must be an object",
            )
        return session

    def _calendar_timestamp(
        self,
        value: Any,
        calendar: str,
        day_key: str,
        field: str,
    ) -> datetime:
        if not isinstance(value, str) or not value:
            raise ValueError(f"{calendar} calendar session {day_key} is missing {field}")
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)
        except ValueError as exc:
            raise ValueError(
                f"{calendar} calendar session {day_key} has invalid {field}: {value}",
            ) from exc

    def _plan_list(self, key: str) -> list[dict[str, Any]]:
        value = self.config.compiled_plan.get(key, [])
        return value if isinstance(value, list) else []

    def _features(self) -> list[dict[str, Any]]:
        return self._plan_list("features")

    def _params(self) -> dict[str, Any]:
        return dict(self.config.parameters or {})

    def _number(self, value: Any, default: float = 0.0) -> float:
        if isinstance(value, dict) and "param" in value:
            return self._number(self._params().get(str(value["param"])), default)
        if value is None:
            return default
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            return default
        return numeric if isfinite(numeric) else default

    def _condition(self, condition: dict[str, Any], offset: int) -> bool:
        kind = str(condition.get("kind", "")).lower()
        handlers = {
            "all": self._condition_all,
            "any": self._condition_any,
            "not": self._condition_not,
            "crosses_above": self._condition_cross,
            "crosses_below": self._condition_cross,
            "compare": self._condition_compare,
            "position_state": self._condition_position_state,
        }
        if handler := handlers.get(kind):
            return handler(condition, offset)
        return False

    def _condition_all(self, condition: dict[str, Any], offset: int) -> bool:
        return all(
            self._condition(child, offset) for child in condition.get("conditions", []) or []
        )

    def _condition_any(self, condition: dict[str, Any], offset: int) -> bool:
        return any(
            self._condition(child, offset) for child in condition.get("conditions", []) or []
        )

    def _condition_not(self, condition: dict[str, Any], offset: int) -> bool:
        children = condition.get("conditions", []) or []
        return len(children) == 1 and not self._condition(children[0], offset)

    def _condition_cross(self, condition: dict[str, Any], offset: int) -> bool:
        kind = str(condition.get("kind", "")).lower()
        left_now = self._value(condition.get("left"), offset)
        right_now = self._value(condition.get("right"), offset)
        left_prev = self._value(condition.get("left"), offset + 1)
        right_prev = self._value(condition.get("right"), offset + 1)
        if None in {left_now, right_now, left_prev, right_prev}:
            return False
        if kind == "crosses_above":
            return bool(left_prev <= right_prev and left_now > right_now)
        return bool(left_prev >= right_prev and left_now < right_now)

    def _condition_compare(self, condition: dict[str, Any], offset: int) -> bool:
        left = self._value(condition.get("left"), offset)
        right = self._value(condition.get("right"), offset)
        if left is None or right is None:
            return False
        operator = _COMPARE_OPERATORS.get(str(condition.get("op")))
        return bool(operator(left, right)) if operator else False

    def _condition_position_state(self, condition: dict[str, Any], offset: int) -> bool:
        del offset
        state = str(condition.get("state", "")).lower()
        qty = self._position_qty()
        if state == "flat":
            return qty == 0
        if state == "long":
            return qty > 0
        return False

    def _value(self, ref: Any, offset: int) -> float | None:
        if not isinstance(ref, dict):
            return None
        kind = str(ref.get("kind", "")).lower()
        if kind == "feature":
            return self._feature_value(str(ref.get("featureId")), offset)
        if kind == "bar_field":
            return self._bar_field(str(ref.get("field", "close")), offset)
        if kind == "constant":
            return self._number(ref.get("value"))
        if kind == "param":
            return self._number(self._params().get(str(ref.get("name"))))
        return None

    def _bar_field(self, field: str, offset: int) -> float | None:
        idx = len(self._bars) - 1 - offset
        if idx < 0:
            return None
        return self._bars[idx].get(field)

    def _feature_value(self, feature_id: str, offset: int) -> float | None:
        key = (feature_id, offset)
        if key in self._feature_cache:
            return self._feature_cache[key]
        feature = next((item for item in self._features() if item.get("id") == feature_id), None)
        if feature is None:
            self._feature_cache[key] = None
            return None
        value = self._compute_feature(feature, offset)
        self._feature_cache[key] = value
        return value

    def _compute_feature(self, feature: dict[str, Any], offset: int) -> float | None:
        kind = str(feature.get("kind", "")).lower()
        field = str(feature.get("field", "close")).lower()
        if kind == "price":
            return self._bar_field(field, offset)

        period = max(1, int(self._number(feature.get("period"), 1)))
        values = self._series(field, offset, period + 1 if kind == "rsi" else period)
        if len(values) < (period + 1 if kind == "rsi" else period):
            return None

        if kind == "sma":
            return sum(values[-period:]) / period
        if kind == "ema":
            return self._ema(field, period, offset)
        if kind == "rsi":
            return self._rsi(values[-(period + 1) :], period)
        if kind == "bollinger":
            return self._bollinger(feature, values[-period:], period)
        if kind == "rolling_high":
            return max(values[-period:])
        if kind == "rolling_low":
            return min(values[-period:])
        return None

    def _bollinger(
        self,
        feature: dict[str, Any],
        values: list[float],
        period: int,
    ) -> float | None:
        middle = sum(values) / period
        std_dev = pstdev(values) if len(values) > 1 else 0.0
        width = self._number(feature.get("stdDev"), 2.0) * std_dev
        band = str(feature.get("band", "middle")).lower()
        if band == "upper":
            return middle + width
        if band == "lower":
            return middle - width
        return middle

    def _series(self, field: str, offset: int, count: int) -> list[float]:
        end = len(self._bars) - offset
        if end <= 0:
            return []
        start = max(0, end - count)
        return [float(bar[field]) for bar in self._bars[start:end] if field in bar]

    def _ema(self, field: str, period: int, offset: int) -> float | None:
        end = len(self._bars) - offset
        if end < period:
            return None
        values = [float(bar[field]) for bar in self._bars[:end] if field in bar]
        if len(values) < period:
            return None
        ema = sum(values[:period]) / period
        alpha = 2.0 / (period + 1.0)
        for value in values[period:]:
            ema = (value * alpha) + (ema * (1.0 - alpha))
        return ema

    def _rsi(self, values: list[float], period: int) -> float | None:
        if len(values) < period + 1:
            return None
        gains: list[float] = []
        losses: list[float] = []
        for prev, current in pairwise(values):
            change = current - prev
            gains.append(max(change, 0.0))
            losses.append(abs(min(change, 0.0)))
        avg_gain = sum(gains[-period:]) / period
        avg_loss = sum(losses[-period:]) / period
        if avg_loss == 0:
            return 100.0
        rs = avg_gain / avg_loss
        return 100.0 - (100.0 / (1.0 + rs))

    def _execute_action(self, action: dict[str, Any], source: str) -> None:
        kind = str(action.get("kind", "")).lower()
        if not self._can_trade_today():
            self._decision(source, False, "max trades per day reached")
            return
        if kind == "set_target_weight":
            self._set_target_weight(self._number(action.get("weight")), source)
        elif kind == "buy_available_cash_pct":
            self._buy_notional(self._available_cash() * self._number(action.get("percent")), source)
        elif kind == "buy_fixed_notional":
            self._buy_notional(self._number(action.get("amount")), source)
        elif kind == "exit_position":
            self._exit_position(source)

    def _set_target_weight(self, weight: float, source: str) -> None:
        weight = max(0.0, min(weight, self._max_position_weight()))
        price = self._last_price()
        if price <= 0:
            return
        current_qty = self._position_qty()
        current_notional = current_qty * price
        target_notional = self._equity_estimate() * weight
        delta = target_notional - current_notional
        if delta > 0:
            self._buy_notional(delta, source)
        elif delta < 0:
            self._sell_quantity(abs(delta) / price, source)

    def _buy_notional(self, notional: float, source: str) -> None:
        cash = max(0.0, self._available_cash() - self._cash_reserve())
        notional = min(max(0.0, notional), cash)
        price = self._last_price()
        if notional <= 0 or price <= 0:
            return
        max_notional = self._equity_estimate() * self._max_position_weight()
        current_notional = self._position_qty() * price
        notional = min(notional, max(0.0, max_notional - current_notional))
        if notional <= 0:
            return
        self._submit_market(OrderSide.BUY, notional / price, source)

    def _exit_position(self, source: str) -> None:
        self._sell_quantity(self._position_qty(), source)

    def _sell_quantity(self, quantity: float, source: str) -> None:
        quantity = min(max(0.0, quantity), self._position_qty())
        if quantity <= 0:
            return
        self._submit_market(OrderSide.SELL, quantity, source)

    def _submit_market(self, side: OrderSide, quantity_value: float, source: str) -> None:
        instrument = self.cache.instrument(self.config.instrument_id)
        if instrument is None:
            self.log.warning("No instrument in cache; skipping order")
            return
        quantity = instrument.make_qty(Decimal(str(quantity_value)), round_down=True)
        if quantity.is_zero():
            self._decision(source, False, "quantity rounded to zero")
            return
        order = self.order_factory.market(
            instrument_id=self.config.instrument_id,
            order_side=side,
            quantity=quantity,
        )
        self.submit_order(order)
        self._record_trade_today()
        self._decision(source, True, f"submitted {side.name} {quantity}")

    def _last_price(self) -> float:
        return self._bar_field("close", 0) or 0.0

    def _position_qty(self) -> float:
        positions = self.cache.positions_open(instrument_id=self.config.instrument_id)
        return sum(max(0.0, float(getattr(position, "signed_qty", 0.0))) for position in positions)

    def _available_cash(self) -> float:
        account = self.portfolio.account(self.config.instrument_id.venue)
        if account is None:
            return 0.0
        balances = account.balances_free()
        money = balances.get(Currency.from_str(self.config.base_currency))
        return float(money.as_double()) if money is not None else 0.0

    def _total_cash(self) -> float:
        account = self.portfolio.account(self.config.instrument_id.venue)
        if account is None:
            return 0.0
        balances = account.balances_total()
        money = balances.get(Currency.from_str(self.config.base_currency))
        return float(money.as_double()) if money is not None else 0.0

    def _equity_estimate(self) -> float:
        return self._total_cash() + (self._position_qty() * self._last_price())

    def _cash_reserve(self) -> float:
        risk = self.config.compiled_plan.get("risk", {})
        reserve_pct = self._number(risk.get("minCashReservePct"), 0.0)
        return max(0.0, reserve_pct) * self._equity_estimate()

    def _max_position_weight(self) -> float:
        risk = self.config.compiled_plan.get("risk", {})
        return max(0.0, min(self._number(risk.get("maxPositionWeight"), 1.0), 1.0))

    def _can_trade_today(self) -> bool:
        risk = self.config.compiled_plan.get("risk", {})
        max_trades = risk.get("maxTradesPerDay")
        if max_trades is None:
            return True
        day = self._current_day()
        return self._trades_today.get(day, 0) < int(max_trades)

    def _record_trade_today(self) -> None:
        day = self._current_day()
        self._trades_today[day] = self._trades_today.get(day, 0) + 1

    def _current_day(self) -> str:
        ts_ns = self._current_ts_ns()
        if ts_ns is None:
            return "unknown"
        return datetime.fromtimestamp(ts_ns / 1_000_000_000, UTC).strftime("%Y-%m-%d")

    def _in_run_window(self) -> bool:
        current = self._current_datetime_utc()
        if current is None:
            return True
        return current >= self._run_start_utc() and self._within_end_time(current)

    def _current_ts_ns(self) -> int | None:
        if self._last_event_ts_ns is not None:
            return self._last_event_ts_ns
        if self._bars:
            return int(self._bars[-1]["ts_event"])
        return None

    def _current_datetime_utc(self) -> datetime | None:
        ts_ns = self._current_ts_ns()
        if ts_ns is None:
            return None
        return datetime.fromtimestamp(ts_ns / 1_000_000_000, UTC)

    def _run_start_utc(self) -> datetime:
        try:
            return datetime.fromisoformat(self.config.start_time.replace("Z", "+00:00")).astimezone(
                UTC,
            )
        except ValueError:
            return self.clock.utc_now().astimezone(UTC)

    def _run_end_utc(self) -> datetime | None:
        if not self.config.end_time:
            return None
        try:
            return datetime.fromisoformat(self.config.end_time.replace("Z", "+00:00")).astimezone(
                UTC,
            )
        except ValueError:
            return None

    def _within_end_time(self, current: datetime) -> bool:
        end = self._run_end_utc()
        return end is None or current <= end

    def _event_datetime_utc(self, event: TimeEvent) -> datetime:
        return datetime.fromtimestamp(int(event.ts_event) / 1_000_000_000, UTC)

    def _decision(self, rule_id: str, result: bool, detail: str) -> None:
        self.log.info(f"decision rule={rule_id} result={result} detail={detail}")
