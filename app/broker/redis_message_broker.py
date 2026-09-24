from __future__ import annotations

import configparser
import json
import logging
from collections.abc import AsyncGenerator
from typing import Any, cast

from redis.asyncio import Redis
from redis.asyncio.client import PubSub
from redis.exceptions import ConnectionError as RedisConnectionError

from app.broker.message_broker import MessageBroker, OverflowPolicy
from app.shared.enums.broker_channels import BrokerChannels
from db.redis_storage import RedisStorageSingleton as RedisStorage


class RedisMessageBroker(MessageBroker):
    """
    RedisMessageBroker is responsible for managing Redis-based pub/sub messaging.
    It handles publishing, subscribing, and broadcasting messages using a
    Redis backend.
    """

    def __init__(
        self,
        config: configparser.ConfigParser | None = None,
        logger: logging.Logger | None = None,
        redis_store: RedisStorage | None = None,
    ) -> None:
        super().__init__(config, logger)
        self.redis_store = redis_store or RedisStorage(self.config, self.logger)
        self.redis: Redis | None = None
        self._active_pubsubs: set[tuple[PubSub, tuple[BrokerChannels, ...]]] = set()
        self.logger.info("RedisMessageBroker initialized.")

    async def connect(self) -> None:
        """
        Establish a connection to the Redis server using the provided
        or default Redis store.

        Logs connection status and handles connection errors gracefully.
        """
        try:
            await self.redis_store.connect()
            self.redis = await self.redis_store.get_client()
            self.logger.info("Broker Connected to Redis successfully.")
        except RedisConnectionError as e:
            err_msg = "Error connecting to Redis in Broker. Please check your Redis server configuration."
            self.logger.error(f"{err_msg}: {e}")
            raise RedisConnectionError(err_msg) from e

    def _get_full_channel(self, game_id: str, channel: BrokerChannels) -> str:
        """
        Generate the full Redis channel name using game_id and channel.

        Args:
            game_id (str): The game identifier to namespace the channel.
            channel (BrokerChannels): The channel to be used in the Redis channel.

        Returns:
            str: The full channel name (game_id:channel).
        """
        return f"game:{game_id}:{channel}"

    async def publish(self, game_id: str, channel: BrokerChannels, message: Any) -> int:
        """
        Publish a message to a specific Redis channel scoped by the game_id.

        Args:
            game_id (str): Identifier to scope the Redis channel.
            channel (str): Channel name to publish the message.
            message (Any): Data to be serialized and sent.

        Returns:
            int: Number of clients that received the message.

        Raises:
            RedisConnectionError: If Redis is not connected.
        """
        full_channel = self._get_full_channel(game_id, channel)
        if self.redis is None:
            await self.connect()

        if self.redis is None:
            raise RedisConnectionError("Redis is not connected.")

        try:
            num_clients = await self.redis.publish(full_channel, json.dumps(message))
            return cast(int, num_clients)
        except Exception as e:
            self.logger.error(f"Broker Failed to publish message: {e}")
            raise

    async def subscribe(
        self,
        game_id: str,
        channels: BrokerChannels | list[BrokerChannels],
        *,
        policy: OverflowPolicy = OverflowPolicy.BLOCK,
        maxsize: int | None = None,
    ) -> AsyncGenerator[Any, None]:
        """
        Subscribe to one or more channels for a specific game_id,
        and yield incoming messages.

        ``policy`` and ``maxsize`` are accepted for interface parity; Redis
        pub/sub is fire-and-forget and applies no client-side backpressure.

        Args:
            game_id (str): Game identifier used to namespace the channel.
            channels (BrokerChannels | list[BrokerChannels]): Single or multiple
                                                                 channel enums.

        Returns:
            AsyncGenerator[Any, None]: Yields decoded messages from the
                                        subscribed channels.

        Raises:
            RedisConnectionError: If Redis is not connected.
        """
        if self.redis is None:
            await self.connect()
        channels_list: tuple[BrokerChannels, ...]
        if isinstance(channels, BrokerChannels):
            channels_list = (channels,)
        elif len(channels) == 0:

            async def empty_generator() -> AsyncGenerator[Any, None]:
                yield

            return empty_generator()
        else:
            channels_list = tuple(channels)

        client = await self.redis_store.get_client()
        pubsub = client.pubsub()

        for channel in channels_list:
            full_channel = self._get_full_channel(game_id, channel)
            await pubsub.subscribe(full_channel)

        self._active_pubsubs.add((pubsub, channels_list))
        self.logger.info(f"Subscribed to channels: {[f'{game_id}:{ch}' for ch in channels_list]}")

        async def generator() -> AsyncGenerator[Any, None]:
            try:
                async for message in pubsub.listen():
                    self.logger.debug(f"Received msg on channel {message['channel']}: {message}")
                    if message["type"] == "message":
                        try:
                            data = json.loads(message["data"])
                            if isinstance(data, dict) and data.get("__sentinel__"):
                                break
                            yield data
                        except json.JSONDecodeError as e:
                            self.logger.warning(f"Invalid JSON received: {e}")
            finally:
                for channel in channels_list:
                    full_channel = self._get_full_channel(game_id, channel)
                    await pubsub.unsubscribe(full_channel)
                self.logger.info(f"Unsubscribed from channels: {[f'{game_id}:{ch}' for ch in channels_list]}")

        return generator()

    async def shutdown(self) -> None:
        """
        Gracefully shut down all Redis pubsub connections by sending
        a sentinel message to trigger unsubscribes and cleaning
        up resources. Logs shutdown activities.
        """
        if self.redis:
            sentinel_message = json.dumps({"__sentinel__": True})

            for pubsub, channels in self._active_pubsubs:
                for channel in channels:
                    await self.redis.publish(channel, sentinel_message)
                await pubsub.unsubscribe()
                await pubsub.aclose()  # type: ignore

            await self.redis.aclose()
            self._active_pubsubs.clear()
            self.logger.info("RedisMessageBroker shutdown completed.")
