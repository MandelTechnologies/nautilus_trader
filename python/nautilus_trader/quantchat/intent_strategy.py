from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime
from datetime import timedelta
from decimal import Decimal
from math import isfinite
from operator import eq
from operator import ge
from operator import gt
from operator import le
from operator import lt
from operator import ne
from typing import Any
from zoneinfo import ZoneInfo

from nautilus_trader.common.events import TimeEvent
from nautilus_trader.config import StrategyConfig
from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import BarType
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.objects import Currency
from nautilus_trader.quantchat.indicators import FeatureState
from nautilus_trader.quantchat.indicators import build_feature_state
from nautilus_trader.quantchat.wall_clock_schedule import next_wall_clock_fire_time
from nautilus_trader.trading.strategy import Strategy


_COMPARE_OPERATORS = {
    "<": lt,
    "<=": le,
    ">": gt,
    ">=": ge,
    "==": eq,
    "!=": ne,
}
_UNBOUNDED_START_UTC = datetime(1970, 1, 1, tzinfo=UTC)
# Indicators are incremental, so raw bar history only serves bar_field refs at
# cross offsets (<= 1), model_signal lags (<= 10, compiler-enforced), and the
# last price/timestamp. A fixed bound keeps memory flat regardless of uptime and
# makes a restarted strategy's reachable state identical to a fresh boot's.
_BAR_HISTORY_MAXLEN = 64
# Model signals are stored per bar timestamp; lookups reach back at most
# lag (<= 10) + cross offset bars, so this comfortably out-sizes the deploy
# preload while keeping memory flat.
_MODEL_SIGNAL_STORE_MAXLEN = 256
# Local msgbus topic carrying live model predictions from the data client.
# Must match adapters.quantchat.constants.MODEL_SIGNAL_TOPIC (the strategy
# must not import the adapter).
_MODEL_SIGNAL_TOPIC = "data.quantchat.model_signal"
_SUPPORTED_RUNTIME_CONTRACT = "quantchat_strategy_intent_v4"


def _extract_model_signal_value(outputs: Any, output: str) -> float | None:
    if not isinstance(outputs, dict):
        return None
    try:
        numeric = float(outputs.get(output))
    except (TypeError, ValueError):
        return None
    return numeric if isfinite(numeric) else None


def _parse_utc_datetime(value: str, field_name: str) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)
    except ValueError as exc:
        raise ValueError(f"runtimeBindings.{field_name} must be an ISO-8601 timestamp") from exc


def _validate_runtime_contract(compiled_plan: dict[str, Any]) -> None:
    contract = str(compiled_plan.get("runtimeContractVersion", ""))
    if contract != _SUPPORTED_RUNTIME_CONTRACT:
        raise ValueError(
            f"Unsupported runtime contract {contract!r}; supported: {_SUPPORTED_RUNTIME_CONTRACT}",
        )


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
    # Seed for the evaluation-time signal store: {ts_event_ns_str: {modelVersionId:
    # outputs}}. The backtest covers its whole window; live deploys preload recent
    # predictions and live messages extend the store.
    model_signals: dict[str, Any] | None = None
    # Total transaction cost in basis points the runtime charges on fills (paper:
    # price slippage; backtest: taker commission). Sizing reserves this headroom.
    cost_bps: float = 0.0
    # Restart state (live only): startup actions already ran for this bot, trades
    # already executed today (UTC), and historical bars to warm indicators with.
    startup_actions_completed: bool = False
    trades_today: int = 0
    warmup_bars: list[dict[str, float]] | None = None


class QuantChatIntentStrategyConfig(StrategyConfig, frozen=True):
    instrument_id: InstrumentId
    bar_type: BarType
    symbol: str
    timeframe: str
    base_currency: str
    start_time: str
    end_time: str
    market_calendar: dict[str, Any]
    model_signals: dict[str, Any]
    compiled_plan: dict[str, Any]
    parameters: dict[str, Any]
    cost_bps: float
    startup_actions_completed: bool
    trades_today: int
    warmup_bars: list[dict[str, float]]


def build_intent_strategy(
    runtime: QuantChatRuntime,
    compiled_plan: dict[str, Any],
    parameters: dict[str, Any],
) -> Strategy:
    _validate_runtime_contract(compiled_plan)
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
            model_signals=runtime.model_signals or {},
            compiled_plan=compiled_plan,
            parameters=parameters,
            cost_bps=runtime.cost_bps,
            startup_actions_completed=runtime.startup_actions_completed,
            trades_today=runtime.trades_today,
            warmup_bars=runtime.warmup_bars or [],
        ),
    )


class QuantChatIntentStrategy(Strategy):
    def __init__(self, config: QuantChatIntentStrategyConfig) -> None:
        super().__init__(config)
        self._bars: deque[dict[str, Any]] = deque(maxlen=_BAR_HISTORY_MAXLEN)
        self._startup_done = bool(config.startup_actions_completed)
        self._feature_states = self._build_feature_states()
        # Evaluation-time signal store: ts_event_ns -> {modelVersionId: outputs}.
        # Consulted when conditions evaluate, so a signal arriving after its bar
        # (but before the lagged evaluation) is still seen — bars never snapshot
        # signals. Missing entries evaluate as condition-false.
        self._model_signals: dict[int, dict[str, Any]] = {}
        for ts_key, outputs_by_version in (config.model_signals or {}).items():
            if isinstance(outputs_by_version, dict):
                self._model_signals[int(ts_key)] = dict(outputs_by_version)
        self._trades_today: dict[str, int] = {}
        self._last_event_ts_ns: int | None = None
        self._run_start_utc_value = (
            _parse_utc_datetime(config.start_time, "startTime") or _UNBOUNDED_START_UTC
        )
        self._run_end_utc_value = _parse_utc_datetime(config.end_time, "endTime")
        if (
            config.start_time
            and self._run_end_utc_value is not None
            and self._run_end_utc_value < self._run_start_utc_value
        ):
            raise ValueError("runtimeBindings.endTime must be greater than or equal to startTime")
        self._last_wall_clock_fire_at: dict[str, datetime] = {}
        self._next_wall_clock_fire_at: dict[str, datetime] = {}

    def on_start(self) -> None:
        self._validate_wall_clock_calendars()
        self._seed_warmup_bars()
        self._seed_trades_today()
        self.subscribe_bars(self.config.bar_type)
        if any(
            str(feature.get("kind", "")).lower() == "model_signal" for feature in self._features()
        ):
            self.msgbus.subscribe(_MODEL_SIGNAL_TOPIC, self._on_model_signal)
        self._setup_wall_clock_triggers()
        self.log.info(
            "QuantChat intent strategy started "
            f"start_time={self._run_start_utc().isoformat()} "
            f"end_time={self._run_end_utc().isoformat() if self._run_end_utc() else 'none'} "
            f"warmup_bars={len(self._bars)} "
            f"startup_done={self._startup_done}",
        )

    def _build_feature_states(self) -> dict[str, FeatureState]:
        """
        Build one incremental indicator state per plan feature.

        Bad settings (unknown kind, out-of-range period) fail here — loudly, at boot —
        never silently mid-run.

        """
        states: dict[str, FeatureState] = {}
        for feature in self._features():
            kind = str(feature.get("kind", "")).lower()
            if kind in {"price", "model_signal"}:
                continue  # Read straight from bar history; no derived state.
            states[str(feature.get("id"))] = build_feature_state(
                kind=kind,
                field=str(feature.get("field", "close")).lower(),
                period=self._number(feature.get("period"), 1.0),
                std_dev=self._number(feature.get("stdDev"), 2.0),
                band=str(feature.get("band", "middle")).lower(),
            )
        return states

    def _append_bar(self, record: dict[str, Any]) -> None:
        """
        Ingest one bar into history and every indicator state.

        Warmup seeding and live bars both flow through here, so indicator state in a
        freshly booted strategy is built exactly as it would have been bar by bar.

        """
        self._bars.append(record)
        for state in self._feature_states.values():
            state.update(record)

    def _seed_warmup_bars(self) -> None:
        """
        Seed bar history from the deploy config so indicators are warm from the first
        live bar.

        Warmup bars are history: they never trigger startup actions or rules.

        """
        for bar in self.config.warmup_bars:
            self._append_bar(dict(bar))

    def _seed_trades_today(self) -> None:
        """
        Resume today's (UTC) trade count so maxTradesPerDay survives restarts.
        """
        if self.config.trades_today > 0:
            day = self.clock.utc_now().astimezone(UTC).strftime("%Y-%m-%d")
            self._trades_today[day] = self.config.trades_today

    def on_bar(self, bar: Bar) -> None:
        self._append_bar(
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

        if not self._in_run_window():
            return

        if not self._startup_done:
            self._startup_done = True
            actions = self._plan_list("startupActions")
            for action in actions:
                self._execute_action(action, "startup")
            self._emit_runtime_event(
                "startup_actions_completed",
                {"action_count": len(actions)},
            )

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
        fired_at = self._event_datetime_utc(event)
        self._last_wall_clock_fire_at[rule_id] = fired_at
        self.log.info(f"wall_clock fired rule={rule_id} event_time={fired_at.isoformat()}")
        self._emit_runtime_event(
            "wall_clock_fired",
            {
                "rule_id": rule_id,
                "fired_at": fired_at.isoformat(),
            },
            fired_at,
        )
        rule = next((item for item in self._plan_list("rules") if item.get("id") == rule_id), None)
        if rule is None:
            self.log.warning(f"Wall-clock rule not found: {rule_id}")
            return
        if self._in_run_window():
            self._evaluate_rule(rule, "wall_clock")
        else:
            self._decision(
                rule_id,
                False,
                "outside run window "
                f"event_time={fired_at.isoformat()} "
                f"start_time={self._run_start_utc().isoformat()} "
                f"end_time={self._run_end_utc().isoformat() if self._run_end_utc() else 'none'}",
            )
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
        rule_id = str(rule.get("id", "rule"))
        if next_time is None:
            self._next_wall_clock_fire_at.pop(rule_id, None)
            self.log.info(
                "wall_clock exhausted "
                f"rule={rule_id} after={after_utc.isoformat()} "
                f"reason=no_next_fire trigger={trigger}",
            )
            return
        if not self._within_end_time(next_time):
            self._next_wall_clock_fire_at.pop(rule_id, None)
            self.log.info(
                "wall_clock exhausted "
                f"rule={rule_id} next_fire_at={next_time.isoformat()} "
                f"end_time={self._run_end_utc().isoformat() if self._run_end_utc() else 'none'}",
            )
            return
        self._next_wall_clock_fire_at[rule_id] = next_time
        timezone_name = str(trigger.get("timezone", "UTC"))
        local_time = next_time.astimezone(ZoneInfo(timezone_name)).isoformat()
        self.log.info(
            "wall_clock scheduled "
            f"rule={rule_id} next_fire_at={next_time.isoformat()} "
            f"timezone={timezone_name} local_time={local_time}",
        )
        self._emit_runtime_event(
            "wall_clock_scheduled",
            {
                "rule_id": rule_id,
                "next_fire_at": next_time.isoformat(),
                "local_fire_at": local_time,
                "timezone": timezone_name,
            },
        )
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
        return next_wall_clock_fire_time(
            trigger,
            after_utc,
            self._wall_clock_calendar_allows,
        )

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
        if offset < 0 or offset >= len(self._bars):
            return None
        return self._bars[-1 - offset].get(field)

    def _feature_value(self, feature_id: str, offset: int) -> float | None:
        state = self._feature_states.get(feature_id)
        if state is not None:
            return state.value_at(offset)
        feature = next((item for item in self._features() if item.get("id") == feature_id), None)
        if feature is None:
            return None
        kind = str(feature.get("kind", "")).lower()
        if kind == "model_signal":
            return self._model_signal_value(feature, offset)
        if kind == "price":
            return self._bar_field(str(feature.get("field", "close")).lower(), offset)
        return None

    def _on_model_signal(self, payload: Any) -> None:
        """
        Insert a live model prediction into the evaluation-time store.
        """
        if not isinstance(payload, dict):
            return
        try:
            ts_event = int(payload["ts_event"])
            model_version_id = str(payload["modelVersionId"])
            outputs = payload["outputs"]
        except (KeyError, TypeError, ValueError):
            self.log.warning(f"Dropping malformed model signal payload: {payload}")
            return
        if not isinstance(outputs, dict):
            self.log.warning("Dropping model signal payload without outputs object")
            return
        self._model_signals.setdefault(ts_event, {})[model_version_id] = outputs
        while len(self._model_signals) > _MODEL_SIGNAL_STORE_MAXLEN:
            del self._model_signals[min(self._model_signals)]
        self.log.info(f"Model signal stored: version={model_version_id} ts_event={ts_event}")

    def _model_signal_value(self, feature: dict[str, Any], offset: int) -> float | None:
        # The signal for bar T only exists after T closes, so evaluation reads the
        # signal stamped lag bars back (compiler enforces lag >= 1).
        lag = max(1, int(self._number(feature.get("lag"), 1.0)))
        idx = len(self._bars) - 1 - offset - lag
        if idx < 0:
            return None
        ts_event = int(self._bars[idx]["ts_event"])
        by_version = self._model_signals.get(ts_event)
        if not isinstance(by_version, dict):
            self.log.warning(
                f"model signal lookup miss: ts={ts_event} idx={idx} bars={len(self._bars)} "
                f"store={len(self._model_signals)} "
                f"store_range={[min(self._model_signals), max(self._model_signals)] if self._model_signals else []}",
            )
            return None
        outputs = by_version.get(str(feature.get("modelVersionId", "")))
        output = str(feature.get("output", "prob_up"))
        return _extract_model_signal_value(outputs, output)

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
            self._decision(source, False, "no last price")
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
        price = self._last_price()
        if price <= 0:
            self._decision(source, False, "no last price")
            return
        if notional <= 0:
            self._decision(source, False, "requested buy notional is zero")
            return
        if cash <= 0:
            self._decision(source, False, "no available cash after reserve")
            return
        # Reserve headroom for transaction costs so a full-cash buy stays affordable
        # at the worst-case fill: the backtest venue's L1 ask sits one tick above the
        # bar close, and both runtimes charge cost_bps on the fill notional, so size
        # such that qty * (px + tick) * (1 + cost) <= cash.
        instrument = self.cache.instrument(self.config.instrument_id)
        tick = float(instrument.price_increment) if instrument is not None else 0.0
        cost_rate = max(0.0, self.config.cost_bps) / 10_000.0
        notional = min(notional, cash * price / ((price + tick) * (1.0 + cost_rate)))
        max_notional = self._equity_estimate() * self._max_position_weight()
        current_notional = self._position_qty() * price
        notional = min(notional, max(0.0, max_notional - current_notional))
        if notional <= 0:
            self._decision(source, False, "max position weight reached")
            return
        self._submit_market(OrderSide.BUY, notional / price, source)

    def _exit_position(self, source: str) -> None:
        self._sell_quantity(self._position_qty(), source)

    def _sell_quantity(self, quantity: float, source: str) -> None:
        quantity = min(max(0.0, quantity), self._position_qty())
        if quantity <= 0:
            self._decision(source, False, "no position to sell")
            return
        self._submit_market(OrderSide.SELL, quantity, source)

    def _submit_market(self, side: OrderSide, quantity_value: float, source: str) -> None:
        instrument = self.cache.instrument(self.config.instrument_id)
        if instrument is None:
            self.log.warning("No instrument in cache; skipping order")
            self._decision(source, False, "no instrument in cache")
            return
        quantity = instrument.make_qty(Decimal(str(quantity_value)), round_down=True)
        if quantity.as_double() == 0.0:
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
        return self._run_start_utc_value

    def _run_end_utc(self) -> datetime | None:
        return self._run_end_utc_value

    def _within_end_time(self, current: datetime) -> bool:
        end = self._run_end_utc()
        return end is None or current <= end

    def _event_datetime_utc(self, event: TimeEvent) -> datetime:
        return datetime.fromtimestamp(int(event.ts_event) / 1_000_000_000, UTC)

    def _emit_runtime_event(
        self,
        event_type: str,
        data: dict[str, Any],
        event_time: datetime | None = None,
    ) -> None:
        timestamp = (
            event_time or self._current_datetime_utc() or self.clock.utc_now().astimezone(UTC)
        )
        payload = {
            "type": event_type,
            "ts_event": int(timestamp.timestamp() * 1_000_000_000),
            **data,
        }
        try:
            self.msgbus.publish("events.quantchat.runtime", payload, external_pub=False)
        except Exception as exc:
            self.log.warning(f"Failed to emit runtime event {event_type}: {exc}")

    def _decision(self, rule_id: str, result: bool, detail: str) -> None:
        self.log.info(f"decision rule={rule_id} result={result} detail={detail}")
        self._emit_runtime_event(
            "decision_evaluated",
            {
                "rule_id": rule_id,
                "result": result,
                "detail": detail,
            },
        )
