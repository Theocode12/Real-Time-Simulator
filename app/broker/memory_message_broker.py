from __future__ import annotations

import asyncio
import configparser
import logging
from collections import defaultdict
from collections.abc import AsyncGenerator
from typing import Any

from app.broker.message_broker import MessageBroker, OverflowPolicy
from app.shared.enums.broker_channels import BrokerChannels


class InMemoryMessageBroker(MessageBroker):
    """
    In-memory message broker using asyncio queues for lightweight pub/sub.

    Publishing is non-blocking: each subscriber declares an ``OverflowPolicy``
    so a slow consumer can never throttle a producer (e.g. the game scheduler).
    Subscribers are stored as ``{game_id: {channel: {queue: policy}}}``.
    """

    _DEFAULT_QUEUE_SIZE = 200

    def __init__(
        self,
        config: configparser.ConfigParser | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        super().__init__(config, logger)
        self._subscribers: dict[str, dict[str, dict[asyncio.Queue[Any], OverflowPolicy]]] = defaultdict(
            lambda: defaultdict(dict)
        )
        self._dropped_messages = 0
        self._shutdown = asyncio.Event()
        self.logger.info("InMemoryMessageBroker initialized.")

    def _queue_size(self, maxsize: int | None) -> int:
        if maxsize is not None:
            return max(1, maxsize)
        return max(1, self.config.getint("broker", "queueSize", fallback=self._DEFAULT_QUEUE_SIZE))

    def _record_drop(self, game_id: str, channel: BrokerChannels) -> None:
        self._dropped_messages += 1
        if self._dropped_messages == 1 or self._dropped_messages % 100 == 0:
            self.logger.warning(
                "InMemoryMessageBroker dropped %s message(s) so far (game=%s channel=%s).",
                self._dropped_messages,
                game_id,
                channel,
            )

    @staticmethod
    def _evict_oldest(queue: asyncio.Queue[Any]) -> None:
        try:
            queue.get_nowait()
        except asyncio.QueueEmpty:
            pass

    def _put_guaranteed(self, queue: asyncio.Queue[Any], message: Any) -> None:
        """Enqueue, evicting oldest messages if needed (used for sentinels)."""
        while True:
            try:
                queue.put_nowait(message)
                return
            except asyncio.QueueFull:
                try:
                    queue.get_nowait()
                except asyncio.QueueEmpty:
                    return

    async def publish(self, game_id: str, channel: BrokerChannels, message: Any) -> int:
        """
        Publish a message to a specific game_id and channel.

        Returns the number of subscribers the message was delivered to. Saturation
        is handled per-subscriber according to its overflow policy, so this never
        blocks on a slow consumer unless the subscriber explicitly opted in.
        """
        if self._shutdown.is_set():
            self.logger.warning("Publish ignored: InMemoryMessageBroker is shutting down.")
            return 0

        subscribers = self._subscribers.get(game_id, {}).get(channel, {})
        if not subscribers:
            return 0

        is_sentinel = isinstance(message, dict) and bool(message.get("__sentinel__"))
        delivered = 0

        for queue, policy in list(subscribers.items()):
            try:
                if is_sentinel:
                    # End-of-stream markers must always be delivered.
                    self._put_guaranteed(queue, message)
                else:
                    queue.put_nowait(message)
                delivered += 1
            except asyncio.QueueFull:
                if policy == OverflowPolicy.BLOCK:
                    try:
                        await queue.put(message)
                        delivered += 1
                    except Exception as e:  # pragma: no cover - defensive
                        self.logger.error(
                            "InMemoryMessageBroker: Failed to publish to %s:%s: %s",
                            game_id,
                            channel,
                            e,
                            exc_info=e,
                        )
                elif policy == OverflowPolicy.DROP_OLD:
                    self._evict_oldest(queue)
                    self._record_drop(game_id, channel)
                    try:
                        queue.put_nowait(message)
                        delivered += 1
                    except asyncio.QueueFull:
                        self._record_drop(game_id, channel)
                else:  # DROP_NEW
                    self._record_drop(game_id, channel)

        return delivered

    async def subscribe(
        self,
        game_id: str,
        channels: BrokerChannels | list[BrokerChannels],
        *,
        policy: OverflowPolicy = OverflowPolicy.BLOCK,
        maxsize: int | None = None,
    ) -> AsyncGenerator[Any, None]:
        """
        Subscribe to one or more channels for a given game_id.

        Args:
            game_id: Game identifier for namespacing.
            channels: One or more channels to subscribe.
            policy: Overflow behaviour for this subscriber's queue.
            maxsize: Queue size (defaults to the ``[broker] queueSize`` setting).

        Returns:
            AsyncGenerator[Any, None]: Yields messages from the subscribed channels.
        """
        if isinstance(channels, BrokerChannels):
            channels_list = [channels]
        elif len(channels) == 0:

            async def empty_generator() -> AsyncGenerator[Any, None]:
                yield

            return empty_generator()
        else:
            channels_list = channels

        queue: asyncio.Queue[Any] = asyncio.Queue(maxsize=self._queue_size(maxsize))

        self.logger.info(
            f"InMemoryMessageBroker: Subscribing to channels for game_id={game_id}, "
            f"channels={channels_list}, policy={policy}"
        )

        for channel in channels_list:
            self._subscribers[game_id][channel][queue] = policy

        async def generator() -> AsyncGenerator[Any, None]:
            try:
                while not self._shutdown.is_set():
                    try:
                        message = await asyncio.wait_for(queue.get(), timeout=1.0)
                        if isinstance(message, dict) and message.get("__sentinel__"):
                            break
                        self.logger.debug(f"InMemoryMessageBroker: Received message {message}.")
                        yield message
                    except asyncio.TimeoutError:
                        continue
                    except asyncio.CancelledError as e:
                        raise e
            finally:
                self._unsubscribe(game_id, channels_list, queue)

        return generator()

    def _unsubscribe(self, game_id: str, channels: list[BrokerChannels], queue: asyncio.Queue[Any]) -> None:
        """Unsubscribe a queue from all specified channels under a game_id."""
        self.logger.debug(f"Unsubscribing queue from channels :{channels}. Game id {game_id}.")
        channel_map = self._subscribers.get(game_id)
        if not channel_map:
            return

        for channel in channels:
            subscriber_queues = channel_map.get(channel)
            if subscriber_queues:
                subscriber_queues.pop(queue, None)
                if not subscriber_queues:
                    del channel_map[channel]

        if not channel_map:
            self._subscribers.pop(game_id, None)
        self.logger.debug(f"Unsubscribe by listener completed for game_id {game_id}.")

    async def shutdown(self) -> None:
        """
        Gracefully shut down the broker by signaling all queues with a sentinel value
        and clearing all subscription data.
        """
        if self._shutdown.is_set():
            return

        self._shutdown.set()
        self.logger.info("InMemoryMessageBroker: Shutdown initiated.")

        all_queues: set[asyncio.Queue[Any]] = set()
        for game_channels in self._subscribers.values():
            for channel_queues in game_channels.values():
                all_queues.update(channel_queues)

        # Actively unblock consumers (evicting if a queue is saturated).
        for queue in all_queues:
            self._put_guaranteed(queue, {"__sentinel__": True})

        self._subscribers.clear()
        self.logger.info("InMemoryMessageBroker: Shutdown completed.")
