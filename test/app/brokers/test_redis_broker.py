from __future__ import annotations

import asyncio
import json
import logging
import uuid
from collections.abc import AsyncGenerator
from configparser import ConfigParser
from typing import Any

import pytest

from app.broker.redis_message_broker import RedisMessageBroker
from app.shared.enums.broker_channels import BrokerChannels


@pytest.fixture
async def live_redis_broker(
    is_redis_live: bool, valid_config: ConfigParser, dummy_logger: logging.Logger
) -> AsyncGenerator[RedisMessageBroker, None]:
    if not is_redis_live:
        pytest.skip("Redis server is not available — skipping live Redis tests.")

    broker = RedisMessageBroker(config=valid_config, logger=dummy_logger)
    await broker.connect()
    yield broker
    await broker.shutdown()


@pytest.mark.asyncio
async def test_publish_and_subscribe(live_redis_broker: RedisMessageBroker) -> None:
    game_id = str(uuid.uuid4())
    channel = BrokerChannels.SCORES_UPDATE
    test_data = {"event": "test", "payload": "hello"}

    async def listener() -> Any:
        # Start the subscription process
        gen = await live_redis_broker.subscribe(game_id, [BrokerChannels.SCORES_UPDATE])
        try:
            async for message in gen:
                return message
        except Exception as e:
            pytest.fail(f"Unexpected error while listening for message: {e!s}")

    # Start listener
    listener_task = asyncio.create_task(listener())

    # Wait briefly for subscription to register
    await asyncio.sleep(0.1)

    # Publish message
    await live_redis_broker.publish(game_id, channel, test_data)

    # Receive message
    message = await listener_task
    assert message == test_data


class _FakePubSub:
    """Minimal pubsub double: records subscribe/unsubscribe/aclose calls."""

    def __init__(self, messages: list[dict[str, Any]] | None = None) -> None:
        self._messages = messages or []
        self.subscribed: list[str] = []
        self.unsubscribed: list[str] = []
        self.unsubscribe_all_calls = 0
        self.aclosed = False

    async def subscribe(self, channel: str) -> None:
        self.subscribed.append(channel)

    async def unsubscribe(self, *channels: str) -> None:
        if channels:
            self.unsubscribed.extend(channels)
        else:
            self.unsubscribe_all_calls += 1

    async def listen(self) -> AsyncGenerator[dict[str, Any], None]:
        for message in self._messages:
            yield message

    async def aclose(self) -> None:
        self.aclosed = True


class _FakeRedis:
    def __init__(self, pubsub: _FakePubSub) -> None:
        self._pubsub = pubsub
        self.published: list[tuple[str, str]] = []

    async def publish(self, channel: Any, message: str) -> int:
        self.published.append((str(channel), message))
        return 1

    def pubsub(self) -> _FakePubSub:
        return self._pubsub

    async def aclose(self) -> None:
        return None


class _FakeRedisStore:
    def __init__(self, redis: _FakeRedis) -> None:
        self._redis = redis

    async def connect(self) -> None:
        return None

    async def get_client(self) -> _FakeRedis:
        return self._redis


def _fake_broker(
    valid_config: ConfigParser, dummy_logger: logging.Logger, messages: list[dict[str, Any]] | None = None
) -> tuple[RedisMessageBroker, _FakePubSub, _FakeRedis]:
    pubsub = _FakePubSub(messages)
    redis = _FakeRedis(pubsub)
    broker = RedisMessageBroker(config=valid_config, logger=dummy_logger, redis_store=_FakeRedisStore(redis))  # type: ignore[arg-type]
    return broker, pubsub, redis


@pytest.mark.asyncio
async def test_subscribe_cleanup_discards_pubsub_entry(
    valid_config: ConfigParser, dummy_logger: logging.Logger
) -> None:
    broker, pubsub, _ = _fake_broker(valid_config, dummy_logger)
    gen = await broker.subscribe("g1", [BrokerChannels.SCORES_UPDATE])

    assert len(broker._active_pubsubs) == 1

    async for _ in gen:
        pass

    assert broker._active_pubsubs == set()
    assert pubsub.unsubscribed == ["game:g1:scores_update"]
    assert pubsub.aclosed is True


@pytest.mark.asyncio
async def test_shutdown_publishes_sentinel_to_full_channel(
    valid_config: ConfigParser, dummy_logger: logging.Logger
) -> None:
    broker, pubsub, redis = _fake_broker(valid_config, dummy_logger)
    await broker.subscribe("g1", [BrokerChannels.SCORES_UPDATE])

    await broker.shutdown()

    assert len(redis.published) == 1
    channel, payload = redis.published[0]
    assert channel == "game:g1:scores_update"
    assert json.loads(payload) == {"__sentinel__": True}
    assert broker._active_pubsubs == set()
    assert pubsub.aclosed is True


@pytest.mark.asyncio
async def test_listen_breaks_on_shutdown_sentinel(valid_config: ConfigParser, dummy_logger: logging.Logger) -> None:
    sentinel = {"type": "message", "channel": "game:g1:scores_update", "data": json.dumps({"__sentinel__": True})}
    broker, _, _ = _fake_broker(valid_config, dummy_logger, [sentinel])
    gen = await broker.subscribe("g1", [BrokerChannels.SCORES_UPDATE])

    with pytest.raises(StopAsyncIteration):
        await anext(gen)

    assert broker._active_pubsubs == set()
