# -------------------------------------------------------------------------------------------------
#  QuantChat Local Paper Trading Adapter for Nautilus Trader
#  https://github.com/mandeltechnologies/quantchat.com
# -------------------------------------------------------------------------------------------------

from __future__ import annotations

import asyncio
from collections.abc import Callable

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
        new = [channel for channel in channels if channel not in self._channels]
        if not new:
            return
        self._channels.update(new)
        self._channels_changed.set()
        if self._pubsub:
            try:
                await self._pubsub.subscribe(*new)
            except Exception as e:
                # The listen loop reconnects and re-subscribes everything tracked.
                self._log.debug(f"Deferred subscribe to reconnect: {e}")

    async def unsubscribe(self, *channels: str) -> None:
        tracked = [channel for channel in channels if channel in self._channels]
        if not tracked:
            return
        self._channels.difference_update(tracked)
        if self._pubsub:
            try:
                await self._pubsub.unsubscribe(*tracked)
            except Exception as e:
                self._log.debug(f"Unsubscribe failed (connection resetting): {e}")

    async def _run(self) -> None:
        backoff = _INITIAL_BACKOFF_SECS
        while True:
            if not self._channels:
                self._channels_changed.clear()
                await self._channels_changed.wait()
                continue

            try:
                pubsub = self._redis.pubsub(ignore_subscribe_messages=True)
                self._pubsub = pubsub
                await pubsub.subscribe(*self._channels)
                self._log.info(f"Redis pub/sub subscribed: {sorted(self._channels)}")
                backoff = _INITIAL_BACKOFF_SECS

                while True:
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
