# -------------------------------------------------------------------------------------------------
#  QuantChat Local Paper Trading Adapter for Nautilus Trader
#  https://github.com/mandeltechnologies/quantchat.com
# -------------------------------------------------------------------------------------------------

from __future__ import annotations

import asyncio
from collections.abc import Callable
import time

import redis.asyncio as aioredis


_INITIAL_BACKOFF_SECS = 1.0
_MAX_BACKOFF_SECS = 30.0
_POLL_TIMEOUT_SECS = 2.0


class ResilientPubSub:
    """
    Redis pub/sub subscription that survives connection loss.

    A market-data feed for live trading must never die silently: on any connection
    error the listen loop reconnects with capped exponential backoff and re-subscribes
    every tracked channel. Handler errors are logged per message and never tear down
    the connection.

    Parameters
    ----------
    redis_url : str
        The Redis connection URL.
    handler : Callable[[str, str], None]
        Callback invoked with (channel, payload) for every received message.
    log
        A Nautilus logger adapter (``self._log`` of the owning client).

    """

    def __init__(
        self,
        redis_url: str,
        handler: Callable[[str, str], None],
        log,
    ) -> None:
        self._redis_url = redis_url
        self._handler = handler
        self._log = log

        self._redis: aioredis.Redis | None = None
        self._pubsub: aioredis.client.PubSub | None = None
        self._listen_task: asyncio.Task | None = None
        self._channels: set[str] = set()
        self._channels_changed = asyncio.Event()

    async def start(self) -> None:
        # Explicit socket_timeout=None: a pub/sub read blocks for as long as the channel
        # is quiet (bars arrive once per minute), so any client-level read timeout would
        # poison the subscription with spurious reconnects.
        self._redis = aioredis.from_url(
            self._redis_url,
            decode_responses=True,
            socket_timeout=None,
        )
        self._listen_task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        if self._listen_task:
            self._listen_task.cancel()
            try:
                await self._listen_task
            except asyncio.CancelledError:
                pass
            self._listen_task = None

        await self._close_pubsub()

        if self._redis:
            await self._redis.close()
            self._redis = None

        self._channels.clear()

    async def subscribe(self, *channels: str) -> None:
        # Only track the change here: the listen loop owns the PubSub object and
        # applies channel changes between polls. Issuing SUBSCRIBE from this task
        # can race the loop's own connection setup — two concurrent commands on a
        # not-yet-connected PubSub split onto two connections, and messages for
        # the losing channels land on a connection nobody polls.
        new = [channel for channel in channels if channel not in self._channels]
        if not new:
            return
        self._channels.update(new)
        self._channels_changed.set()

    async def unsubscribe(self, *channels: str) -> None:
        tracked = [channel for channel in channels if channel in self._channels]
        if not tracked:
            return
        self._channels.difference_update(tracked)
        self._channels_changed.set()

    async def _run(self) -> None:
        backoff = _INITIAL_BACKOFF_SECS
        while True:
            if not self._channels:
                self._channels_changed.clear()
                await self._channels_changed.wait()
                continue

            self._channels_changed.clear()
            try:
                pubsub = self._redis.pubsub(ignore_subscribe_messages=True)
                self._pubsub = pubsub
                await pubsub.subscribe(*self._channels)
                self._log.info(f"Redis pub/sub subscribed: {sorted(self._channels)}")
                backoff = _INITIAL_BACKOFF_SECS

                # Poll until the tracked channel set changes, then rebuild the
                # subscription from scratch (changes only happen around startup).
                while not self._channels_changed.is_set():
                    # Bounded poll instead of a blocking listen() so a quiet channel is
                    # indistinguishable from a healthy one regardless of socket timeouts;
                    # get_message returns None when the poll window elapses.
                    message = await pubsub.get_message(timeout=_POLL_TIMEOUT_SECS)
                    if message is None or message["type"] != "message":
                        continue
                    try:
                        self._handler(message["channel"], message["data"])
                    except Exception as e:
                        self._log.error(
                            f"Error handling message on {message['channel']}: {e}",
                        )
                await self._close_pubsub()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self._log.error(
                    f"Redis pub/sub connection lost: {e}; reconnecting in {backoff:.0f}s",
                )
                await self._close_pubsub()
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2.0, _MAX_BACKOFF_SECS)

    async def _close_pubsub(self) -> None:
        if self._pubsub:
            try:
                await self._pubsub.close()
            except Exception as e:
                self._log.debug(f"Error closing pub/sub connection: {e}")
            self._pubsub = None


class ResilientStreamReader:
    """
    Redis stream reader that survives connection loss without losing entries.

    Unlike pub/sub, a stream read resumes from the last delivered entry id after a
    reconnect, so anything appended while the connection was down is delivered on
    recovery instead of silently lost. Used for model signals, where a skipped
    message would make a strategy evaluate a condition as false.

    Parameters
    ----------
    redis_url : str
        The Redis connection URL.
    handler : Callable[[str, str], None]
        Callback invoked with (stream_key, payload) for every entry's ``payload`` field.
    log
        A Nautilus logger adapter (``self._log`` of the owning client).
    lookback_ms : int
        How far before reader start the first read reaches. The deploy preload
        already covers history, so this only needs to bridge the gap between the
        preload snapshot and the reader coming up; replayed entries are idempotent.

    """

    def __init__(
        self,
        redis_url: str,
        handler: Callable[[str, str], None],
        log,
        lookback_ms: int = 300_000,
    ) -> None:
        self._redis_url = redis_url
        self._handler = handler
        self._log = log
        self._lookback_ms = lookback_ms

        self._redis: aioredis.Redis | None = None
        self._read_task: asyncio.Task | None = None
        self._last_ids: dict[str, str] = {}
        self._streams_changed = asyncio.Event()

    async def start(self) -> None:
        self._redis = aioredis.from_url(
            self._redis_url,
            decode_responses=True,
            socket_timeout=None,
        )
        self._read_task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        if self._read_task:
            self._read_task.cancel()
            try:
                await self._read_task
            except asyncio.CancelledError:
                pass
            self._read_task = None

        if self._redis:
            await self._redis.close()
            self._redis = None

        self._last_ids.clear()

    def add_streams(self, *keys: str) -> None:
        start_id = f"{max(int(time.time() * 1000) - self._lookback_ms, 0)}-0"
        new = [key for key in keys if key not in self._last_ids]
        if not new:
            return
        for key in new:
            self._last_ids[key] = start_id
        self._streams_changed.set()

    async def _run(self) -> None:
        backoff = _INITIAL_BACKOFF_SECS
        while True:
            if not self._last_ids:
                self._streams_changed.clear()
                await self._streams_changed.wait()
                continue

            self._streams_changed.clear()
            try:
                # Bounded block so stream-set changes are picked up promptly and a
                # quiet stream never looks like a dead connection.
                response = await self._redis.xread(
                    dict(self._last_ids),
                    block=int(_POLL_TIMEOUT_SECS * 1000),
                )
                backoff = _INITIAL_BACKOFF_SECS
                for stream_key, entries in response or []:
                    for entry_id, fields in entries:
                        self._last_ids[stream_key] = entry_id
                        payload = fields.get("payload")
                        if payload is None:
                            continue
                        try:
                            self._handler(stream_key, payload)
                        except Exception as e:
                            self._log.error(
                                f"Error handling stream entry {entry_id} on {stream_key}: {e}",
                            )
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self._log.error(
                    f"Redis stream read failed: {e}; retrying in {backoff:.0f}s",
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2.0, _MAX_BACKOFF_SECS)
