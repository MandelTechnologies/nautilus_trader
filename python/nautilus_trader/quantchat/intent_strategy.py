from __future__ import annotations

from bisect import bisect_left
from collections import deque
from dataclasses import dataclass
from datetime import UTC
from datetime import date
from datetime import datetime
from datetime import timedelta
from decimal import Decimal
import json
from math import isfinite
from operator import eq
from operator import ge
from operator import gt
from operator import le
from operator import lt
from operator import ne
from typing import Any
from zoneinfo import ZoneInfo

from signal_engine import SignalEngine
from signal_engine import adjustment_factors

from nautilus_trader.common.events import TimeEvent
from nautilus_trader.config import StrategyConfig
from nautilus_trader.core.uuid import UUID4
from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import BarType
from nautilus_trader.model.enums import ContingencyType
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import OrderType
from nautilus_trader.model.enums import TimeInForce
from nautilus_trader.model.enums import TrailingOffsetType
from nautilus_trader.model.enums import TriggerType
from nautilus_trader.model.events import OrderFilled
from nautilus_trader.model.events import OrderRejected
from nautilus_trader.model.events import PositionClosed
from nautilus_trader.model.identifiers import ClientOrderId
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.objects import Currency
from nautilus_trader.model.orders import LimitOrder
from nautilus_trader.model.orders import Order
from nautilus_trader.model.orders import StopMarketOrder
from nautilus_trader.quantchat.event_relative_schedule import next_event_relative_fire
from nautilus_trader.quantchat.event_relative_schedule import next_session_anchor_fire
from nautilus_trader.quantchat.wall_clock_schedule import next_wall_clock_fire_time
from nautilus_trader.trading.strategy import Strategy


_TIME_IN_FORCE = {
    "gtc": TimeInForce.GTC,
    "day": TimeInForce.DAY,
    "ioc": TimeInForce.IOC,
    "fok": TimeInForce.FOK,
}


_COMPARE_OPERATORS = {
    "<": lt,
    "<=": le,
    ">": gt,
    ">=": ge,
    "==": eq,
    "!=": ne,
}
_UNBOUNDED_START_UTC = datetime(1970, 1, 1, tzinfo=UTC)


def _bar_civil_date(record: dict[str, Any]) -> date:
    """
    UTC civil date of a bar record, for ex-date boundary checks.
    """
    return datetime.fromtimestamp(float(record["ts_event"]) / 1_000_000_000, UTC).date()


# Derived series live in the signal engine, so raw bar history only serves
# bar_field refs at cross offsets (<= 1), the previous bar's timestamp for
# model-signal feeds, and the last price. A fixed bound keeps memory flat
# regardless of uptime and makes a restarted strategy's reachable state
# identical to a fresh boot's.
_BAR_HISTORY_MAXLEN = 64
# Model signals are stored per bar timestamp; lookups reach back at most
# lag (<= 10) + cross offset bars, so this comfortably out-sizes the deploy
# preload while keeping memory flat.
_MODEL_SIGNAL_STORE_MAXLEN = 256
# Local msgbus topic carrying live model predictions from the data client.
# Must match adapters.quantchat.constants.MODEL_SIGNAL_TOPIC (the strategy
# must not import the adapter).
_MODEL_SIGNAL_TOPIC = "data.quantchat.model_signal"
_SUPPORTED_RUNTIME_CONTRACTS = {
    "quantchat_strategy_intent_v4",
    "quantchat_strategy_intent_v5",
    "quantchat_strategy_intent_v6",
    "quantchat_strategy_intent_v7",
}

# Closed set an `ActionDefV1.orderType` may take (backend-rs mod.rs ORDER_TYPES).
_ORDER_TYPES = {"market", "limit", "stop", "stop_limit", "bracket", "trailing_stop"}

# Selector symbol placeholder resolved to the run's catalyst symbol (the
# underlying's symbol, which per-symbol catalysts are keyed by).
_INSTRUMENT_PLACEHOLDER = "$INSTRUMENT"


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
    if contract not in _SUPPORTED_RUNTIME_CONTRACTS:
        raise ValueError(
            "Unsupported runtime contract "
            f"{contract!r}; supported: {', '.join(sorted(_SUPPORTED_RUNTIME_CONTRACTS))}",
        )


def _parse_event_datetime(value: Any, field_name: str) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)
    except ValueError as exc:
        raise ValueError(f"runtimeBindings.catalystCalendar.{field_name} is invalid") from exc


def _event_day_start(value: datetime) -> datetime:
    return datetime(value.year, value.month, value.day, tzinfo=UTC)


def _event_day_end(value: datetime) -> datetime:
    return _event_day_start(value) + timedelta(days=1, microseconds=-1)


def _event_active_at(event: dict[str, Any], current: datetime) -> bool:
    event_date = _parse_event_datetime(event.get("eventDate"), "events[].eventDate")
    if event_date is None:
        return False
    end_date = _parse_event_datetime(event.get("endDate"), "events[].endDate") or event_date
    return _event_day_start(event_date) <= current.astimezone(UTC) <= _event_day_end(end_date)


def _selector_matches_event(selector: dict[str, Any], event: dict[str, Any]) -> bool:
    if not isinstance(selector, dict) or not isinstance(event, dict):
        return False
    catalyst_type = str(selector.get("type", "")).upper()
    symbol = str(selector.get("symbol", "")).upper()
    if catalyst_type and catalyst_type != str(event.get("type", "")).upper():
        return False
    if symbol and symbol != str(event.get("symbol", "")).upper():
        return False

    details = selector.get("details") or {}
    event_details = event.get("details") or {}
    if not isinstance(details, dict) or not isinstance(event_details, dict):
        return False
    for key, expected in details.items():
        actual = event_details.get(str(key))
        if actual is None:
            return False
        actual_value = str(actual).lower()
        if isinstance(expected, list):
            allowed = {str(item).lower() for item in expected}
        else:
            allowed = {str(expected).lower()}
        if actual_value not in allowed:
            return False
    return True


@dataclass(frozen=True)
class QuantChatRuntime:
    instrument_id: InstrumentId
    bar_type: BarType
    symbol: str
    timeframe: str
    # The underlying's symbol, which per-symbol catalysts are keyed by
    # (differs from `symbol` for crypto pairs). Resolves $instrument event
    # selectors.
    catalyst_symbol: str = ""
    base_currency: str = "USD"
    start_time: str = ""
    end_time: str = ""
    market_calendar: dict[str, Any] | None = None
    catalyst_calendar: dict[str, Any] | None = None
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
    # Splits and dividends for the bound instrument over the fed series
    # ({exDate, kind, factor|amount, payDate?}). Present (possibly empty) only
    # for plans that requested the dual-series bar policy (§8.1).
    corporate_actions: list[dict[str, Any]] | None = None


class QuantChatIntentStrategyConfig(StrategyConfig, frozen=True):
    instrument_id: InstrumentId
    bar_type: BarType
    symbol: str
    timeframe: str
    catalyst_symbol: str
    base_currency: str
    start_time: str
    end_time: str
    market_calendar: dict[str, Any]
    catalyst_calendar: dict[str, Any]
    model_signals: dict[str, Any]
    compiled_plan: dict[str, Any]
    parameters: dict[str, Any]
    cost_bps: float
    startup_actions_completed: bool
    trades_today: int
    warmup_bars: list[dict[str, float]]
    corporate_actions: list[dict[str, Any]]


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
            catalyst_symbol=runtime.catalyst_symbol,
            base_currency=runtime.base_currency,
            start_time=runtime.start_time,
            end_time=runtime.end_time,
            market_calendar=runtime.market_calendar or {},
            catalyst_calendar=runtime.catalyst_calendar or {},
            model_signals=runtime.model_signals or {},
            compiled_plan=compiled_plan,
            parameters=parameters,
            cost_bps=runtime.cost_bps,
            startup_actions_completed=runtime.startup_actions_completed,
            trades_today=runtime.trades_today,
            warmup_bars=runtime.warmup_bars or [],
            corporate_actions=runtime.corporate_actions or [],
        ),
    )


class QuantChatIntentStrategy(Strategy):
    def __init__(self, config: QuantChatIntentStrategyConfig) -> None:
        super().__init__(config)
        self._bars: deque[dict[str, Any]] = deque(maxlen=_BAR_HISTORY_MAXLEN)
        self._startup_done = bool(config.startup_actions_completed)
        self._engine = self._build_signal_engine()
        self._model_signal_keys = self._build_model_signal_keys()
        # Evaluation-time signal store: ts_event_ns -> {modelVersionId: outputs}.
        # Consulted when conditions evaluate, so a signal arriving after its bar
        # (but before the lagged evaluation) is still seen — bars never snapshot
        # signals. Missing entries evaluate as condition-false.
        self._model_signals: dict[int, dict[str, Any]] = {}
        for ts_key, outputs_by_version in (config.model_signals or {}).items():
            if isinstance(outputs_by_version, dict):
                self._model_signals[int(ts_key)] = dict(outputs_by_version)
        self._catalyst_events = self._load_catalyst_events()
        self._corporate_actions = self._load_corporate_actions()
        self._applied_ca_count = 0
        # Rebuilding the engine at an ex-date needs every bar ever fed (EMA-family
        # state has unbounded memory), so full history is retained exactly when
        # corporate actions exist for the run — zero overhead otherwise.
        self._fed_history: list[tuple[dict[str, Any], dict[str, float] | None]] = []
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
        self._last_event_rule_fire_at: dict[str, datetime] = {}
        self._next_event_rule_fire_at: dict[str, datetime] = {}
        # Bracket specs deferred to PositionOpened (market-type entries whose
        # stop/target prices are expressed relative to the realized fill, not
        # known at submission time). Keyed by the entry order's client_order_id
        # so on_event can match the fill that should trigger leg submission.
        self._pending_brackets: dict[str, dict[str, Any]] = {}
        # Client order IDs of resting stop/target legs per instrument, so a
        # position close (e.g. an opposite signal) cancels the sibling leg
        # instead of leaving an orphaned resting order.
        self._bracket_order_ids: set[str] = set()

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
        self._setup_event_triggers()
        self.log.info(
            "QuantChat intent strategy started "
            f"start_time={self._run_start_utc().isoformat()} "
            f"end_time={self._run_end_utc().isoformat() if self._run_end_utc() else 'none'} "
            f"warmup_bars={len(self._bars)} "
            f"startup_done={self._startup_done}",
        )

    def on_event(self, event: Any) -> None:
        """
        Submit deferred bracket legs once the entry that spawned them fills (PINNED
        semantic: stop/target prices on a market-type bracket entry are relative to
        the realized fill, so they cannot be computed at submission time), cancel any
        resting bracket leg orphaned by a position close, and report order rejections
        that would otherwise be silent (e.g. `reject_stop_orders` firing because a
        stop's trigger is already through the current market on submission).

        """
        if isinstance(event, OrderFilled):
            if not self._pending_brackets or event.instrument_id != self.config.instrument_id:
                return
            spec = self._pending_brackets.pop(event.client_order_id.value, None)
            if spec is not None:
                self._submit_bracket_legs(spec, event.position_id)
        elif isinstance(event, PositionClosed):
            if event.instrument_id == self.config.instrument_id:
                self._cancel_bracket_orders()
        elif isinstance(event, OrderRejected):
            self._decision(
                "order_rejected",
                False,
                f"order {event.client_order_id.value} rejected: {event.reason}",
            )

    def _submit_bracket_legs(self, spec: dict[str, Any], position_id: Any) -> None:
        """
        Evaluate stopPrice/targetPrice THEN (position.avgCost is live from the realized
        fill) and submit the OCO stop(+target) pair against the just-opened position.

        A stop-only bracket (no targetPrice) is a legal degenerate case: just the stop
        order, no OCO linkage needed.

        """
        action = spec["action"]
        entry_side = spec["side"]
        quantity = spec["quantity"]
        instrument = self.cache.instrument(self.config.instrument_id)
        if instrument is None:
            return
        exit_side = OrderSide.SELL if entry_side == OrderSide.BUY else OrderSide.BUY

        stop_price = self._price_from_expr(instrument, action.get("stopPrice"))
        if stop_price is None:
            self.log.warning(
                "Bracket stopPrice did not evaluate to a usable price; no exit submitted",
            )
            return
        target_price = self._price_from_expr(instrument, action.get("targetPrice"))

        time_in_force = _TIME_IN_FORCE.get(
            str(action.get("timeInForce", "gtc")).lower(),
            TimeInForce.GTC,
        )

        if target_price is None:
            # Stop-only bracket: a plain resting stop, no OCO sibling.
            stop_order: Order = self.order_factory.stop_market(
                instrument_id=self.config.instrument_id,
                order_side=exit_side,
                quantity=quantity,
                trigger_price=stop_price,
                time_in_force=time_in_force,
                reduce_only=True,
            )
            self._bracket_order_ids.add(stop_order.client_order_id.value)
            self.submit_order(stop_order, position_id=position_id)
            self.log.info(f"Bracket stop-only leg submitted: trigger={stop_price}")
            return

        # Stop + target: OCO pair, mirroring order_factory.bracket()'s internal
        # linkage (the factory itself can't be used here because the entry already
        # filled — this pair attaches to an already-open position).
        stop_client_order_id = self.order_factory.generate_client_order_id()
        target_client_order_id = self.order_factory.generate_client_order_id()

        stop_order = StopMarketOrder(
            trader_id=self.trader_id,
            strategy_id=self.id,
            instrument_id=self.config.instrument_id,
            client_order_id=stop_client_order_id,
            order_side=exit_side,
            quantity=quantity,
            trigger_price=stop_price,
            trigger_type=TriggerType.DEFAULT,
            init_id=UUID4(),
            ts_init=self.clock.timestamp_ns(),
            time_in_force=time_in_force,
            reduce_only=True,
            contingency_type=ContingencyType.OCO,
            linked_order_ids=[target_client_order_id],
            tags=["STOP_LOSS"],
        )
        target_order = LimitOrder(
            trader_id=self.trader_id,
            strategy_id=self.id,
            instrument_id=self.config.instrument_id,
            client_order_id=target_client_order_id,
            order_side=exit_side,
            quantity=quantity,
            price=target_price,
            init_id=UUID4(),
            ts_init=self.clock.timestamp_ns(),
            time_in_force=time_in_force,
            reduce_only=True,
            contingency_type=ContingencyType.OCO,
            linked_order_ids=[stop_client_order_id],
            tags=["TAKE_PROFIT"],
        )
        self._bracket_order_ids.add(stop_client_order_id.value)
        self._bracket_order_ids.add(target_client_order_id.value)
        self.submit_order(stop_order, position_id=position_id)
        self.submit_order(target_order, position_id=position_id)
        self.log.info(
            f"Bracket OCO legs submitted: stop={stop_price} target={target_price}",
        )

    def _cancel_bracket_orders(self) -> None:
        """
        Cancel any still-resting bracket leg once its position closes (either the OCO
        sibling filled, in which case the venue already canceled this one and
        `cancel_order` is a no-op, or an opposite signal exited the position out from
        under a still-resting stop/target).
        """
        if not self._bracket_order_ids:
            return
        for raw_client_order_id in list(self._bracket_order_ids):
            order = self.cache.order(ClientOrderId(raw_client_order_id))
            if order is not None and order.is_open:
                self.cancel_order(order)
        self._bracket_order_ids.clear()

    def _build_signal_engine(self) -> SignalEngine:
        """
        Compile every plan feature (indicators, price fields, model signals) into one
        signal-engine graph.

        The engine owns all derived-series state behind the same offset-read contract
        the legacy per-feature states exposed, with batched and incremental evaluation
        guaranteed bit-identical. Bad settings (unknown kind, out-of-range period) fail
        here — loudly, at boot — never silently mid-run.

        """
        params = {
            str(name): float(value)
            for name, value in self._params().items()
            if isinstance(value, (int, float)) and not isinstance(value, bool)
        }
        return SignalEngine.from_legacy_features(json.dumps(self._features()), params)

    def _build_model_signal_keys(self) -> dict[str, tuple[str, str]]:
        """
        External-stream keys the engine's model-signal graphs read, mapped to
        (modelVersionId, output) for store lookups at feed time.
        """
        keys: dict[str, tuple[str, str]] = {}
        for feature in self._features():
            if str(feature.get("kind", "")).lower() != "model_signal":
                continue
            model_version_id = str(feature.get("modelVersionId", ""))
            output = str(feature.get("output", "prob_up"))
            keys[f"model_signal:{model_version_id}:{output}"] = (model_version_id, output)
        return keys

    def _model_signal_externals(self) -> dict[str, float] | None:
        """
        External samples for the bar being appended.

        The freshest prediction that can exist when a bar closes is the one stamped at
        the previous bar, so that is what feeds the engine each bar; the translated
        model-signal graphs encode deeper lags as lag nodes. Missing predictions are
        simply absent (the engine reads undefined).

        """
        if not self._model_signal_keys or len(self._bars) < 2:
            return None
        ts_event = self._bars[-2].get("ts_event")
        if ts_event is None:
            return None
        by_version = self._model_signals.get(int(ts_event))
        if not isinstance(by_version, dict):
            return None
        externals: dict[str, float] = {}
        for key, (model_version_id, output) in self._model_signal_keys.items():
            value = _extract_model_signal_value(by_version.get(model_version_id), output)
            if value is not None:
                externals[key] = value
        return externals or None

    def _append_bar(self, record: dict[str, Any]) -> None:
        """
        Ingest one bar into history and the signal engine.

        Warmup seeding and live bars both flow through here, so engine state in a
        freshly booted strategy is built exactly as it would have been bar by bar.

        Dual-series bar policy (§8.1): indicator math runs on the back-adjusted series.
        Between corporate actions the newest bars are their own adjusted values (factor
        1.0), so bars feed straight through; when a bar first crosses an ex-date the
        engine is rebuilt over the full adjusted history — batched and incremental
        evaluation are bit-identical, so the rebuild is exact, and runs without
        corporate actions are byte-identical to the raw feed.

        """
        self._bars.append(record)
        externals = self._model_signal_externals()
        if self._corporate_actions:
            self._fed_history.append((record, externals))
            if self._crossed_ex_date(record):
                self._rebuild_engine_adjusted()
                return
        self._engine.step(
            float(record["open"]),
            float(record["high"]),
            float(record["low"]),
            float(record["close"]),
            float(record["volume"]),
            externals,
        )

    def _crossed_ex_date(self, record: dict[str, Any]) -> bool:
        """
        Advance past every corporate action whose ex-date this bar reaches; true when
        the bar opens a new adjustment regime.
        """
        crossed = False
        bar_date = _bar_civil_date(record)
        while (
            self._applied_ca_count < len(self._corporate_actions)
            and self._corporate_actions[self._applied_ca_count]["ex_date"] <= bar_date
        ):
            self._applied_ca_count += 1
            crossed = True
        return crossed

    def _rebuild_engine_adjusted(self) -> None:
        """
        Re-run a fresh engine over the back-adjusted full history.

        The adjustment convention (and its single implementation) is shared with the
        backend preview path via the signal-engine wheel; each action scales all bars
        strictly before its effective bar (first bar at or after the ex-date), so the
        current bar always feeds its raw values.

        """
        dates = [_bar_civil_date(record) for record, _ in self._fed_history]
        closes = [float(record["close"]) for record, _ in self._fed_history]
        events = [
            (
                bisect_left(dates, action["ex_date"]),
                action["kind"],
                action["value"],
            )
            for action in self._corporate_actions[: self._applied_ca_count]
        ]
        price_factors, volume_factors = adjustment_factors(closes, events)
        engine = self._build_signal_engine()
        for (record, externals), price, volume in zip(
            self._fed_history,
            price_factors,
            volume_factors,
            strict=True,
        ):
            engine.step(
                float(record["open"]) * price,
                float(record["high"]) * price,
                float(record["low"]) * price,
                float(record["close"]) * price,
                float(record["volume"]) * volume,
                externals,
            )
        self._engine = engine
        self.log.info(
            f"Signal engine rebuilt on adjusted history: bars={len(self._fed_history)} "
            f"actions_applied={self._applied_ca_count}",
        )

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

    def _load_catalyst_events(self) -> list[dict[str, Any]]:
        calendar = self.config.catalyst_calendar
        if not isinstance(calendar, dict):
            raise ValueError("runtimeBindings.catalystCalendar must be an object")
        raw_events = calendar.get("events", [])
        if not isinstance(raw_events, list):
            raise ValueError("runtimeBindings.catalystCalendar.events must be an array")
        events: list[dict[str, Any]] = []
        for raw in raw_events:
            if not isinstance(raw, dict):
                raise ValueError("runtimeBindings.catalystCalendar.events entries must be objects")
            event = dict(raw)
            details = event.get("details") or {}
            event["details"] = details if isinstance(details, dict) else {}
            if _parse_event_datetime(event.get("eventDate"), "events[].eventDate") is not None:
                events.append(event)
        events.sort(key=lambda event: str(event.get("eventDate", "")))
        return events

    def _load_corporate_actions(self) -> list[dict[str, Any]]:
        """
        Validate the corporateActions runtime binding into `{ex_date, kind, value,
        pay_date}` entries sorted by ex-date.

        The backend controls the shape, so malformed entries fail the boot loudly rather
        than silently dropping an adjustment.

        """
        raw = self.config.corporate_actions
        if not isinstance(raw, list):
            raise ValueError("runtimeBindings.corporateActions must be an array")
        actions: list[dict[str, Any]] = []
        for entry in raw:
            if not isinstance(entry, dict):
                raise ValueError("runtimeBindings.corporateActions entries must be objects")
            kind = str(entry.get("kind", ""))
            try:
                ex_date = date.fromisoformat(str(entry.get("exDate", "")))
                if kind == "split":
                    value = float(entry["factor"])
                elif kind == "dividend":
                    value = float(entry["amount"])
                else:
                    raise ValueError(f"unknown kind: {kind!r}")
            except (KeyError, TypeError, ValueError) as err:
                raise ValueError(f"invalid corporateActions entry {entry!r}: {err}") from err
            actions.append({"ex_date": ex_date, "kind": kind, "value": value})
        actions.sort(key=lambda action: action["ex_date"])
        return actions

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

    def _setup_event_triggers(self) -> None:
        after = self._run_start_utc()
        if not self.config.end_time:
            after = max(after, self.clock.utc_now().astimezone(UTC))
        after -= timedelta(microseconds=1)
        for rule in self._plan_list("rules"):
            kind = self._trigger_kind(rule)
            if kind == "event_occurs":
                self._schedule_next_event_rule(rule, after)
            elif kind == "event_relative":
                self._schedule_next_event_relative_rule(rule, after)
            elif kind in {"session_open", "session_close"}:
                self._schedule_next_session_rule(rule, after)

    def _schedule_next_session_rule(self, rule: dict[str, Any], after_utc: datetime) -> None:
        trigger = rule.get("trigger")
        if not isinstance(trigger, dict):
            return
        rule_id = str(rule.get("id", "rule"))
        kind = self._trigger_kind(rule)
        anchor = "open" if kind == "session_open" else "close"
        session_name = str(trigger.get("session", "")).upper()
        # Named session windows serialize under marketCalendar like venue
        # calendars; a missing map fails loudly at boot.
        sessions = self._calendar_sessions(session_name)
        offset_minutes = int(self._number(trigger.get("offsetMinutes"), 0.0))
        fire_at = next_session_anchor_fire(anchor, offset_minutes, sessions, after_utc)
        if fire_at is None or not self._within_end_time(fire_at):
            self._next_event_rule_fire_at.pop(rule_id, None)
            self.log.info(
                f"{kind} exhausted rule={rule_id} after={after_utc.isoformat()} "
                f"session={session_name}",
            )
            return
        now = self.clock.utc_now().astimezone(UTC)
        if fire_at <= now:
            self._schedule_next_session_rule(rule, now)
            return
        self._next_event_rule_fire_at[rule_id] = fire_at
        self.log.info(
            f"{kind} scheduled rule={rule_id} session={session_name} "
            f"next_fire_at={fire_at.isoformat()}",
        )
        self._emit_runtime_event(
            "event_rule_scheduled",
            {
                "rule_id": rule_id,
                "event_id": session_name,
                "next_fire_at": fire_at.isoformat(),
            },
        )
        alert_name = f"quantchat:session:{rule_id}:{int(fire_at.timestamp())}"
        self.clock.set_time_alert(
            alert_name,
            fire_at,
            lambda time_event, rule_id=rule_id: self._on_session_rule(rule_id, time_event),
            allow_past=False,
        )

    def _on_session_rule(self, rule_id: str, event: TimeEvent) -> None:
        self._last_event_ts_ns = int(event.ts_event)
        fired_at = self._event_datetime_utc(event)
        self._last_event_rule_fire_at[rule_id] = fired_at
        rule = next((item for item in self._plan_list("rules") if item.get("id") == rule_id), None)
        if rule is None:
            self.log.warning(f"Session rule not found: {rule_id}")
            return
        kind = self._trigger_kind(rule)
        self.log.info(f"{kind} fired rule={rule_id} fire_time={fired_at.isoformat()}")
        self._emit_runtime_event(
            "event_rule_fired",
            {
                "rule_id": rule_id,
                "event_id": str(rule.get("trigger", {}).get("session", "")),
                "fired_at": fired_at.isoformat(),
            },
            fired_at,
        )
        if self._in_run_window():
            self._evaluate_rule(rule, kind)
        else:
            self._decision(
                rule_id,
                False,
                "outside run window "
                f"fire_time={fired_at.isoformat()} "
                f"start_time={self._run_start_utc().isoformat()} "
                f"end_time={self._run_end_utc().isoformat() if self._run_end_utc() else 'none'}",
            )
        self._schedule_next_session_rule(rule, fired_at)

    def _resolved_selector(self, selector: dict[str, Any]) -> dict[str, Any]:
        """
        Plan selectors may bind the run's instrument via the $instrument placeholder;
        events always carry concrete symbols.
        """
        if str(selector.get("symbol", "")).upper() != _INSTRUMENT_PLACEHOLDER:
            return selector
        if not self.config.catalyst_symbol:
            raise ValueError(
                "Plan uses the $instrument event selector but the deploy "
                "carries no catalystSymbol runtime binding",
            )
        return {**selector, "symbol": self.config.catalyst_symbol.upper()}

    def _on_event_rule(self, rule_id: str, event_id: str, event: TimeEvent) -> None:
        self._last_event_ts_ns = int(event.ts_event)
        fired_at = self._event_datetime_utc(event)
        self._last_event_rule_fire_at[rule_id] = fired_at
        self.log.info(
            "event_occurs fired "
            f"rule={rule_id} event_id={event_id} event_time={fired_at.isoformat()}",
        )
        self._emit_runtime_event(
            "event_rule_fired",
            {
                "rule_id": rule_id,
                "event_id": event_id,
                "fired_at": fired_at.isoformat(),
            },
            fired_at,
        )
        rule = next((item for item in self._plan_list("rules") if item.get("id") == rule_id), None)
        if rule is None:
            self.log.warning(f"Event rule not found: {rule_id}")
            return
        if self._in_run_window():
            self._evaluate_rule(rule, "event_occurs")
        else:
            self._decision(
                rule_id,
                False,
                "outside run window "
                f"event_time={fired_at.isoformat()} "
                f"start_time={self._run_start_utc().isoformat()} "
                f"end_time={self._run_end_utc().isoformat() if self._run_end_utc() else 'none'}",
            )
        self._schedule_next_event_rule(rule, fired_at)

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

    def _schedule_next_event_rule(self, rule: dict[str, Any], after_utc: datetime) -> None:
        trigger = rule.get("trigger")
        if not isinstance(trigger, dict):
            return
        rule_id = str(rule.get("id", "rule"))
        event = self._next_catalyst_event(trigger, after_utc)
        if event is None:
            self._next_event_rule_fire_at.pop(rule_id, None)
            self.log.info(
                "event_occurs exhausted "
                f"rule={rule_id} after={after_utc.isoformat()} trigger={trigger}",
            )
            return
        next_time = _parse_event_datetime(event.get("eventDate"), "events[].eventDate")
        if next_time is None:
            return
        if not self._within_end_time(next_time):
            self._next_event_rule_fire_at.pop(rule_id, None)
            self.log.info(
                "event_occurs exhausted "
                f"rule={rule_id} next_fire_at={next_time.isoformat()} "
                f"end_time={self._run_end_utc().isoformat() if self._run_end_utc() else 'none'}",
            )
            return
        self._next_event_rule_fire_at[rule_id] = next_time
        event_id = str(event.get("id", ""))
        self.log.info(
            "event_occurs scheduled "
            f"rule={rule_id} event_id={event_id} next_fire_at={next_time.isoformat()}",
        )
        self._emit_runtime_event(
            "event_rule_scheduled",
            {
                "rule_id": rule_id,
                "event_id": event_id,
                "next_fire_at": next_time.isoformat(),
            },
        )
        alert_name = f"quantchat:event:{rule_id}:{event_id}:{int(next_time.timestamp())}"
        self.clock.set_time_alert(
            alert_name,
            next_time,
            lambda time_event, rule_id=rule_id, event_id=event_id: self._on_event_rule(
                rule_id,
                event_id,
                time_event,
            ),
            allow_past=False,
        )

    def _schedule_next_event_relative_rule(
        self,
        rule: dict[str, Any],
        after_utc: datetime,
    ) -> None:
        trigger = rule.get("trigger")
        if not isinstance(trigger, dict):
            return
        rule_id = str(rule.get("id", "rule"))
        selector = trigger.get("selector")
        if not isinstance(selector, dict):
            return
        resolved = self._resolved_selector(selector)
        matched = [
            event for event in self._catalyst_events if _selector_matches_event(resolved, event)
        ]
        calendar = str(trigger.get("calendar", "24/7")).upper()
        sessions = self._calendar_sessions(calendar) if calendar != "24/7" else {}
        fire = next_event_relative_fire(trigger, matched, sessions, after_utc)
        if fire is None or not self._within_end_time(fire.fire_at):
            self._next_event_rule_fire_at.pop(rule_id, None)
            self.log.info(
                "event_relative exhausted "
                f"rule={rule_id} after={after_utc.isoformat()} trigger={trigger}",
            )
            return
        # Live boots compute `after` from now, but belt-and-suspenders: a
        # past alert would hard-crash the clock (allow_past=False).
        now = self.clock.utc_now().astimezone(UTC)
        if fire.fire_at <= now:
            self._schedule_next_event_relative_rule(rule, now)
            return
        self._next_event_rule_fire_at[rule_id] = fire.fire_at
        self.log.info(
            "event_relative scheduled "
            f"rule={rule_id} event_id={fire.event_id} "
            f"next_fire_at={fire.fire_at.isoformat()}",
        )
        self._emit_runtime_event(
            "event_rule_scheduled",
            {
                "rule_id": rule_id,
                "event_id": fire.event_id,
                "next_fire_at": fire.fire_at.isoformat(),
            },
        )
        alert_name = (
            f"quantchat:event_relative:{rule_id}:{fire.event_id}:{int(fire.fire_at.timestamp())}"
        )
        self.clock.set_time_alert(
            alert_name,
            fire.fire_at,
            lambda time_event, rule_id=rule_id, event_id=fire.event_id: (
                self._on_event_relative_rule(rule_id, event_id, time_event)
            ),
            allow_past=False,
        )

    def _on_event_relative_rule(self, rule_id: str, event_id: str, event: TimeEvent) -> None:
        self._last_event_ts_ns = int(event.ts_event)
        fired_at = self._event_datetime_utc(event)
        self._last_event_rule_fire_at[rule_id] = fired_at
        self.log.info(
            "event_relative fired "
            f"rule={rule_id} event_id={event_id} fire_time={fired_at.isoformat()}",
        )
        self._emit_runtime_event(
            "event_rule_fired",
            {
                "rule_id": rule_id,
                "event_id": event_id,
                "fired_at": fired_at.isoformat(),
            },
            fired_at,
        )
        rule = next((item for item in self._plan_list("rules") if item.get("id") == rule_id), None)
        if rule is None:
            self.log.warning(f"Event-relative rule not found: {rule_id}")
            return
        if self._in_run_window():
            self._evaluate_rule(rule, "event_relative")
        else:
            self._decision(
                rule_id,
                False,
                "outside run window "
                f"fire_time={fired_at.isoformat()} "
                f"start_time={self._run_start_utc().isoformat()} "
                f"end_time={self._run_end_utc().isoformat() if self._run_end_utc() else 'none'}",
            )
        self._schedule_next_event_relative_rule(rule, fired_at)

    def _next_catalyst_event(
        self,
        trigger: dict[str, Any],
        after_utc: datetime,
    ) -> dict[str, Any] | None:
        selector = trigger.get("selector")
        if not isinstance(selector, dict):
            return None
        selector = self._resolved_selector(selector)
        for event in self._catalyst_events:
            event_time = _parse_event_datetime(event.get("eventDate"), "events[].eventDate")
            if event_time is None or event_time <= after_utc:
                continue
            if not _selector_matches_event(selector, event):
                continue
            if not self._event_calendar_allows(event_time, trigger):
                continue
            return event
        return None

    def _validate_wall_clock_calendars(self) -> None:
        calendars = {
            str(rule.get("trigger", {}).get("calendar", "24/7")).upper()
            for rule in self._plan_list("rules")
            if self._trigger_kind(rule) in {"wall_clock", "event_occurs", "event_relative"}
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

    def _event_calendar_allows(self, candidate_utc: datetime, trigger: dict[str, Any]) -> bool:
        calendar = str(trigger.get("calendar", "24/7")).upper()
        if calendar == "24/7":
            return True
        candidate_day = candidate_utc.astimezone(ZoneInfo("America/New_York")).date()
        return self._wall_clock_calendar_allows(candidate_day, candidate_utc, trigger)

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

    def _sizing_value(self, expr: Any, offset: int = 0) -> float:
        """
        Evaluate a `SizingExprV1` (backend-rs mod.rs) to a scalar.

        Strategy-layer only: this walks account/position streams, engine features,
        params, and constants. It never runs inside the Rust signal engine, which stays
        pure market data (spec Sec6.3). Bare numbers and `{"param": ...}` are the legacy
        `NumberExprV1` shape and pass straight through to `_number` so every plan
        compiled before this expression tree existed evaluates unchanged.

        """
        if isinstance(expr, (int, float)) or (isinstance(expr, dict) and "param" in expr):
            return self._number(expr)
        if not isinstance(expr, dict):
            return 0.0
        kind = str(expr.get("kind", "")).lower()
        if kind == "constant":
            return self._number(expr.get("value"))
        if kind == "param":
            return self._number(self._params().get(str(expr.get("name"))))
        if kind == "feature_ref":
            return self._feature_value(str(expr.get("featureId")), offset) or 0.0
        if kind == "account_field":
            return self._account_field(str(expr.get("field")))
        if kind == "binary_op":
            return self._sizing_binary_op(expr, offset)
        if kind == "clamp":
            return self._sizing_clamp(expr, offset)
        return 0.0

    def _sizing_binary_op(self, expr: dict[str, Any], offset: int) -> float:
        left = self._sizing_value(expr.get("left"), offset)
        right = self._sizing_value(expr.get("right"), offset)
        ops = {
            "add": lambda: left + right,
            "sub": lambda: left - right,
            "mul": lambda: left * right,
            "div": lambda: (left / right) if right else 0.0,
        }
        compute = ops.get(str(expr.get("op")))
        return compute() if compute else 0.0

    def _sizing_clamp(self, expr: dict[str, Any], offset: int) -> float:
        value = self._sizing_value(expr.get("expr"), offset)
        bound = self._sizing_value(expr.get("bound"), offset)
        return min(value, bound) if str(expr.get("op")) == "min" else max(value, bound)

    def _account_field(self, field: str) -> float:
        if field == "account.equity":
            return self._equity_estimate()
        if field == "account.cash":
            return self._available_cash()
        if field == "position.quantity":
            return self._position_qty()
        if field == "position.avgCost":
            return self._position_avg_cost()
        if field == "position.unrealizedPnl":
            return self._position_unrealized_pnl()
        return 0.0

    def _position_avg_cost(self) -> float:
        """
        Quantity-weighted average open price across every open position on the bound
        instrument (there is at most one under NETTING OMS, but this stays correct if
        that ever changes).
        """
        positions = self.cache.positions_open(instrument_id=self.config.instrument_id)
        total_qty = 0.0
        weighted = 0.0
        for position in positions:
            qty = abs(float(getattr(position, "signed_qty", 0.0)))
            if qty <= 0.0:
                continue
            total_qty += qty
            weighted += qty * float(getattr(position, "avg_px_open", 0.0))
        return weighted / total_qty if total_qty > 0.0 else 0.0

    def _position_unrealized_pnl(self) -> float:
        positions = self.cache.positions_open(instrument_id=self.config.instrument_id)
        instrument = self.cache.instrument(self.config.instrument_id)
        last_price = self._last_price()
        if instrument is None or last_price <= 0.0:
            return 0.0
        price = instrument.make_price(last_price)
        total = 0.0
        for position in positions:
            money = position.unrealized_pnl(price)
            if money is not None:
                total += float(money.as_double())
        return total

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
            "event_active": self._condition_event_active,
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

    def _condition_event_active(self, condition: dict[str, Any], offset: int) -> bool:
        del offset
        selector = condition.get("selector")
        if not isinstance(selector, dict):
            return False
        selector = self._resolved_selector(selector)
        current = self._current_datetime_utc() or self.clock.utc_now().astimezone(UTC)
        return any(
            _selector_matches_event(selector, event) and _event_active_at(event, current)
            for event in self._catalyst_events
        )

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
        if offset < 0:
            return None
        try:
            return self._engine.output(feature_id, offset)
        except ValueError:
            return None  # Unknown feature id: legacy reads resolved to None.

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

    def _execute_action(self, action: dict[str, Any], source: str) -> None:
        kind = str(action.get("kind", "")).lower()
        if self._is_entry_action(action) and not self._entry_guards_allow(source):
            return
        if not self._can_trade_today():
            self._decision(source, False, "max trades per day reached")
            return
        if kind == "set_target_weight":
            self._set_target_weight(self._sizing_value(action.get("weight")), action, source)
        elif kind == "buy_available_cash_pct":
            self._buy_notional(
                self._available_cash() * self._sizing_value(action.get("percent")),
                action,
                source,
            )
        elif kind == "buy_fixed_notional":
            self._buy_notional(self._sizing_value(action.get("amount")), action, source)
        elif kind == "exit_position":
            self._exit_position(source)

    def _entry_guards_allow(self, source: str) -> bool:
        for guard in self._plan_list("entryGuards"):
            if str(guard.get("appliesTo", "entries")).lower() != "entries":
                continue
            condition = guard.get("condition")
            if isinstance(condition, dict) and not self._condition(condition, offset=0):
                guard_id = str(guard.get("id", "entry_guard"))
                self._decision(source, False, f"entry guard blocked: {guard_id}")
                return False
        return True

    def _is_entry_action(self, action: dict[str, Any]) -> bool:
        kind = str(action.get("kind", "")).lower()
        if kind in {"buy_available_cash_pct", "buy_fixed_notional"}:
            return True
        if kind != "set_target_weight":
            return False
        target = max(
            0.0,
            min(self._sizing_value(action.get("weight")), self._max_position_weight()),
        )
        price = self._last_price()
        equity = self._equity_estimate()
        current_weight = (
            (self._position_qty() * price / equity) if price > 0 and equity > 0 else 0.0
        )
        return target > current_weight

    def _set_target_weight(self, weight: float, action: dict[str, Any], source: str) -> None:
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
            self._buy_notional(delta, action, source)
        elif delta < 0:
            self._sell_quantity(abs(delta) / price, source)

    def _buy_notional(self, notional: float, action: dict[str, Any], source: str) -> None:
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
        self._submit_order(OrderSide.BUY, notional / price, action, source)

    def _exit_position(self, source: str) -> None:
        self._sell_quantity(self._position_qty(), source)

    def _sell_quantity(self, quantity: float, source: str) -> None:
        quantity = min(max(0.0, quantity), self._position_qty())
        if quantity <= 0:
            self._decision(source, False, "no position to sell")
            return
        self._submit_order(OrderSide.SELL, quantity, {}, source)

    def _submit_order(
        self,
        side: OrderSide,
        quantity_value: float,
        action: dict[str, Any],
        source: str,
    ) -> None:
        """
        Dispatch on `action.orderType` (default "market").

        Every path submits the entry
        leg now; a `bracket` whose stop/target prices are relative to the entry fill
        (the common case for a market-type entry) defers stop/target submission to
        `on_event`'s `PositionOpened` handler, once `position.avgCost` is live (see
        `SizingExprV1`'s pinned semantic doc in backend-rs mod.rs).

        """
        instrument = self.cache.instrument(self.config.instrument_id)
        if instrument is None:
            self.log.warning("No instrument in cache; skipping order")
            self._decision(source, False, "no instrument in cache")
            return
        quantity = instrument.make_qty(Decimal(str(quantity_value)), round_down=True)
        if quantity.as_double() == 0.0:
            self._decision(source, False, "quantity rounded to zero")
            return

        order_type = str(action.get("orderType", "market")).lower()
        time_in_force = _TIME_IN_FORCE.get(
            str(action.get("timeInForce", "gtc")).lower(),
            TimeInForce.GTC,
        )
        builders = {
            "market": self._build_market_order,
            "limit": self._build_limit_order,
            "stop": self._build_stop_order,
            "stop_limit": self._build_stop_limit_order,
            "trailing_stop": self._build_trailing_stop_order,
            "bracket": self._build_bracket_entry,
        }
        builder = builders.get(order_type)
        if builder is None:
            self._decision(source, False, f"unsupported order type: {order_type}")
            return

        order = builder(instrument, side, quantity, time_in_force, action, source)
        if order is None:
            return  # The builder already recorded a decision (bad price, or a
            # bracket submitted directly as an order list).

        self.submit_order(order)
        self._record_trade_today()
        self._decision(source, True, f"submitted {order_type} {side.name} {quantity}")

    def _build_market_order(self, instrument, side, quantity, time_in_force, action, source):
        del instrument, action, source
        return self.order_factory.market(
            instrument_id=self.config.instrument_id,
            order_side=side,
            quantity=quantity,
            time_in_force=time_in_force,
        )

    def _build_limit_order(self, instrument, side, quantity, time_in_force, action, source):
        price = self._price_from_expr(instrument, action.get("limitPrice"))
        if price is None:
            self._decision(source, False, "limitPrice did not evaluate to a usable price")
            return None
        return self.order_factory.limit(
            instrument_id=self.config.instrument_id,
            order_side=side,
            quantity=quantity,
            price=price,
            time_in_force=time_in_force,
        )

    def _build_stop_order(self, instrument, side, quantity, time_in_force, action, source):
        price = self._price_from_expr(instrument, action.get("stopPrice"))
        if price is None:
            self._decision(source, False, "stopPrice did not evaluate to a usable price")
            return None
        return self.order_factory.stop_market(
            instrument_id=self.config.instrument_id,
            order_side=side,
            quantity=quantity,
            trigger_price=price,
            time_in_force=time_in_force,
        )

    def _build_stop_limit_order(self, instrument, side, quantity, time_in_force, action, source):
        limit_price = self._price_from_expr(instrument, action.get("limitPrice"))
        trigger_price = self._price_from_expr(instrument, action.get("stopPrice"))
        if limit_price is None or trigger_price is None:
            self._decision(source, False, "limitPrice/stopPrice did not evaluate to usable prices")
            return None
        return self.order_factory.stop_limit(
            instrument_id=self.config.instrument_id,
            order_side=side,
            quantity=quantity,
            price=limit_price,
            trigger_price=trigger_price,
            time_in_force=time_in_force,
        )

    def _build_trailing_stop_order(self, instrument, side, quantity, time_in_force, action, source):
        del instrument
        offset = self._sizing_value(action.get("trailingOffset"))
        if offset <= 0:
            self._decision(source, False, "trailingOffset did not evaluate to a positive offset")
            return None
        return self.order_factory.trailing_stop_market(
            instrument_id=self.config.instrument_id,
            order_side=side,
            quantity=quantity,
            trailing_offset=Decimal(str(offset)),
            trailing_offset_type=TrailingOffsetType.PRICE,
            time_in_force=time_in_force,
        )

    def _build_bracket_entry(self, instrument, side, quantity, time_in_force, action, source):
        """
        Submit a `bracket` action's entry: either nautilus's native single-call bracket
        (entry price known at submission time, both stop and target present) or a plain
        entry order with stop/target deferred to `on_event`'s `OrderFilled` handling,
        once `position.avgCost` is live from the realized fill (PINNED semantic — the
        common case for a market-type entry, and for a stop-only bracket regardless of
        entry type).
        """
        entry_price = self._price_from_expr(instrument, action.get("limitPrice"))
        target_price = self._price_from_expr(instrument, action.get("targetPrice"))
        if entry_price is not None and target_price is not None:
            stop_price = self._price_from_expr(instrument, action.get("stopPrice"))
            if stop_price is None:
                self._decision(source, False, "stopPrice did not evaluate to a usable price")
                return None
            order_list = self.order_factory.bracket(
                instrument_id=self.config.instrument_id,
                order_side=side,
                quantity=quantity,
                time_in_force=time_in_force,
                entry_order_type=OrderType.LIMIT,
                entry_price=entry_price,
                sl_trigger_price=stop_price,
                tp_price=target_price,
            )
            self.submit_order_list(order_list)
            self._record_trade_today()
            self._decision(source, True, f"submitted bracket (limit entry) {side.name} {quantity}")
            return None

        order = self.order_factory.market(
            instrument_id=self.config.instrument_id,
            order_side=side,
            quantity=quantity,
            time_in_force=time_in_force,
        )
        self._pending_brackets[order.client_order_id.value] = {
            "action": action,
            "side": side,
            "quantity": quantity,
        }
        return order

    def _price_from_expr(self, instrument: Any, expr: Any) -> Any:
        if expr is None:
            return None
        value = self._sizing_value(expr)
        if value <= 0 or not isfinite(value):
            return None
        return instrument.make_price(value)

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
