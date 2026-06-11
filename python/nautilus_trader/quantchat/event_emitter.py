"""
Event Emitter Actor for quantchat.

Subscribes to trading events within Nautilus and publishes them to Redis for the backend
to persist orders, fills, and positions.

"""

from collections import deque
from datetime import UTC
from datetime import datetime
from decimal import Decimal
import json
import os
from typing import Any

import redis

from nautilus_trader.common.actor import Actor
from nautilus_trader.config import ActorConfig
from nautilus_trader.model.events import OrderAccepted
from nautilus_trader.model.events import OrderCanceled
from nautilus_trader.model.events import OrderFilled
from nautilus_trader.model.events import OrderRejected
from nautilus_trader.model.events import PositionChanged
from nautilus_trader.model.events import PositionClosed
from nautilus_trader.model.events import PositionOpened
from nautilus_trader.model.objects import Price
from nautilus_trader.model.position import Position


# Stream cap bounds Redis memory; at typical bot event rates this is months of
# history, and the backend consumer group acks within seconds.
_STREAM_MAXLEN = 100_000
# Events buffered in-process while Redis is unreachable. Flushed in order on the
# next emission; beyond this the oldest are dropped (bounded memory beats OOM).
_PENDING_MAXLEN = 10_000


class EventEmitterConfig(ActorConfig, frozen=True):
    """
    Configuration for the EventEmitter actor.
    """

    bot_id: str = ""
    redis_url: str = "redis://localhost:6379"


class EventEmitter(Actor):
    """
    Actor that emits trading events to Redis for backend consumption.

    Subscribes to:
    - Order events (filled, rejected, canceled)
    - Position events (opened, changed, closed)

    Appends to the Redis stream ``engine:events`` (the envelope carries bot_id).
    A stream entry survives until the backend consumer group acks it, so a
    backend restart can never lose a fill the way a pub/sub publish could.

    """

    def __init__(self, config: EventEmitterConfig) -> None:
        super().__init__(config)
        self._bot_id = config.bot_id or os.environ.get("QUANTCHAT_BOT_ID", "")
        self._redis_url = config.redis_url or os.environ.get("REDIS_URL", "redis://localhost:6379")
        self._redis: redis.Redis | None = None
        self._stream = "engine:events"
        self._pending: deque[str] = deque(maxlen=_PENDING_MAXLEN)

    def on_start(self) -> None:
        """
        Connect to Redis and subscribe to trading events.
        """
        if not self._bot_id:
            self._log.warning("No bot_id configured, events will not be emitted")
            return

        try:
            self._redis = redis.from_url(self._redis_url, decode_responses=True)
            self._redis.ping()
            self._log.info(f"Connected to Redis, appending events to {self._stream}")
        except Exception as e:
            self._log.error(f"Failed to connect to Redis: {e}")
            self._redis = None
            return

        # Subscribe to all order and position events via message bus
        self.msgbus.subscribe(topic="events.order.*", handler=self._handle_order_event)
        self.msgbus.subscribe(topic="events.position.*", handler=self._handle_position_event)
        self.msgbus.subscribe(topic="events.quantchat.*", handler=self._handle_runtime_event)

        self._log.info("EventEmitter started, subscribed to order, position, and runtime events")

    def on_stop(self) -> None:
        """
        Flush anything still pending, then clean up the Redis connection.
        """
        self._flush_pending()
        if self._pending:
            self._log.error(f"Stopping with {len(self._pending)} unflushed engine events")
        if self._redis:
            try:
                self._redis.close()
            except Exception as e:
                self._log.debug(f"Error closing Redis connection: {e}")
            self._redis = None
        self._log.info("EventEmitter stopped")

    def _publish(self, event_type: str, data: dict[str, Any]) -> None:
        """
        Append an event to the Redis stream, flushing any buffered backlog first.
        """
        if not self._redis:
            return

        now = datetime.now(UTC)
        ts_ns = int(now.timestamp() * 1_000_000_000)

        envelope = {
            "type": event_type,
            "ts": now.isoformat().replace("+00:00", "Z"),
            "ts_ns": ts_ns,
            "bot_id": self._bot_id,
            "data": data,
        }

        self._pending.append(json.dumps(envelope, default=self._json_default))
        self._flush_pending()

    def _flush_pending(self) -> None:
        """
        Append buffered events in order; stop at the first failure so a Redis outage
        delays delivery instead of dropping or reordering it.
        """
        if not self._redis:
            return
        while self._pending:
            payload = self._pending[0]
            try:
                self._redis.xadd(
                    self._stream,
                    {"payload": payload},
                    maxlen=_STREAM_MAXLEN,
                    approximate=True,
                )
            except Exception as e:
                self._log.error(
                    f"Failed to append engine event ({len(self._pending)} pending): {e}",
                )
                return
            self._pending.popleft()
            self._log.info(f"Appended engine event to {self._stream}")

    @staticmethod
    def _json_default(obj: Any) -> Any:
        """
        JSON serializer for objects not serializable by default.
        """
        if isinstance(obj, Decimal):
            return str(obj)
        if hasattr(obj, "to_str"):
            return obj.to_str()
        if hasattr(obj, "__str__"):
            return str(obj)
        raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

    def _handle_order_event(self, event: Any) -> None:
        """
        Handle order events from the message bus.
        """
        self._log.info(f"Received order event: {type(event).__name__}")
        if isinstance(event, OrderFilled):
            self._on_order_filled(event)
        elif isinstance(event, OrderAccepted):
            self._on_order_accepted(event)
        elif isinstance(event, OrderRejected):
            self._on_order_rejected(event)
        elif isinstance(event, OrderCanceled):
            self._on_order_canceled(event)

    def _handle_position_event(self, event: Any) -> None:
        """
        Handle position events from the message bus.
        """
        if isinstance(event, (PositionOpened, PositionChanged, PositionClosed)):
            self._on_position_event(event)

    def _handle_runtime_event(self, event: Any) -> None:
        """
        Handle QuantChat runtime audit events from strategies.
        """
        if not isinstance(event, dict):
            self._log.warning(f"Ignoring unsupported runtime event: {type(event).__name__}")
            return

        event_type = str(event.get("type", ""))
        if event_type not in {
            "wall_clock_scheduled",
            "wall_clock_fired",
            "decision_evaluated",
            "startup_actions_completed",
        }:
            self._log.warning(f"Ignoring unsupported runtime event type: {event_type}")
            return

        self._publish(event_type, {key: value for key, value in event.items() if key != "type"})

    def _on_order_accepted(self, event: OrderAccepted) -> None:
        """
        Handle order accepted event.
        """
        order = self.cache.order(event.client_order_id)
        if order is None:
            self._log.warning(
                f"Order not found in cache for accepted event: {event.client_order_id}",
            )
            return

        self._publish(
            "order_accepted",
            {
                "client_order_id": str(event.client_order_id),
                "venue_order_id": str(event.venue_order_id) if event.venue_order_id else None,
                "instrument_id": str(event.instrument_id),
                "side": order.side.name,
                "qty": str(order.quantity),
                "price": str(order.price) if order.has_price else None,
                "ts_event": event.ts_event,
            },
        )

    def _on_order_filled(self, event: OrderFilled) -> None:
        """
        Handle order filled event.
        """
        self._publish(
            "order_filled",
            {
                "client_order_id": str(event.client_order_id),
                "venue_order_id": str(event.venue_order_id),
                "execution_id": str(event.trade_id),
                "instrument_id": str(event.instrument_id),
                "side": event.order_side.name,
                "qty": str(event.last_qty),
                "price": str(event.last_px),
                "commission": str(event.commission.as_decimal())
                if event.commission is not None
                else None,
                "ts_event": event.ts_event,
            },
        )

    def _on_order_rejected(self, event: OrderRejected) -> None:
        """
        Handle order rejected event.
        """
        self._publish(
            "order_rejected",
            {
                "client_order_id": str(event.client_order_id),
                "instrument_id": str(event.instrument_id),
                "strategy_id": str(event.strategy_id),
                "account_id": str(event.account_id),
                "reason": event.reason,
                "event_id": str(event.id),
                "ts_event": event.ts_event,
            },
        )

    def _on_order_canceled(self, event: OrderCanceled) -> None:
        """
        Handle order canceled event.
        """
        self._publish(
            "order_canceled",
            {
                "client_order_id": str(event.client_order_id),
                "venue_order_id": str(event.venue_order_id) if event.venue_order_id else None,
                "instrument_id": str(event.instrument_id),
                "strategy_id": str(event.strategy_id),
                "account_id": str(event.account_id),
                "event_id": str(event.id),
                "ts_event": event.ts_event,
            },
        )

    def _on_position_event(self, event: PositionOpened | PositionChanged | PositionClosed) -> None:
        """
        Handle position events.
        """
        position: Position | None = self.cache.position(event.position_id)
        if not position:
            return

        event_type = {
            PositionOpened: "position_opened",
            PositionChanged: "position_changed",
            PositionClosed: "position_closed",
        }.get(type(event), "position")

        self._publish(
            event_type,
            {
                "position_id": str(event.position_id),
                "instrument_id": str(event.instrument_id),
                "side": position.side.name,
                "signed_qty": str(position.signed_qty),
                "avg_px_open": str(position.avg_px_open),
                "avg_px_close": str(position.avg_px_close) if position.avg_px_close > 0 else None,
                "realized_pnl": str(position.realized_pnl.as_decimal())
                if position.realized_pnl is not None
                else None,
                "unrealized_pnl": str(
                    position.unrealized_pnl(
                        Price(position.avg_px_open, position.price_precision),
                    ).as_decimal(),
                )
                if position.is_open
                else None,
                "ts_event": event.ts_event,
            },
        )
