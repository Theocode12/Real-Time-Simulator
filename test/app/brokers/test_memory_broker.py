from __future__ import annotations

import asyncio
import configparser
import logging
from typing import Any

import pytest

from app.broker.memory_message_broker import InMemoryMessageBroker
from app.broker.message_broker import OverflowPolicy
from app.shared.enums.broker_channels import BrokerChannels


@pytest.fixture
def broker() -> InMemoryMessageBroker:
    config = configparser.ConfigParser()
    logger = logging.getLogger("test")
    return InMemoryMessageBroker(config=config, logger=logger)


@pytest.mark.asyncio
async def test_publish_to_subscriber(broker: InMemoryMessageBroker) -> None:
    game_id = "game1"
    message = {"scores": [0, 0]}

    gen = await broker.subscribe(game_id, BrokerChannels.SCORES_UPDATE)
    reader_task = asyncio.create_task(anext(gen))

    await asyncio.sleep(0.01)  # Let subscription register
    await broker.publish(game_id, BrokerChannels.SCORES_UPDATE, message)

    received = await reader_task
    assert received == message


@pytest.mark.asyncio
async def test_multiple_subscribers_receive_message(
    broker: InMemoryMessageBroker,
) -> None:
    game_id = "gameX"
    message = {"msg": "multi"}

    gen1 = await broker.subscribe(game_id, BrokerChannels.SCORES_UPDATE)
    gen2 = await broker.subscribe(game_id, BrokerChannels.SCORES_UPDATE)

    task1 = asyncio.create_task(anext(gen1))
    task2 = asyncio.create_task(anext(gen2))

    await broker.publish(game_id, BrokerChannels.SCORES_UPDATE, message)

    result1 = await task1
    result2 = await task2

    assert result1 == result2 == message


@pytest.mark.asyncio
async def test_subscribe_and_unsubscribe_cleanup(
    broker: InMemoryMessageBroker,
) -> None:
    game_id = "game-cleanup"
    channel = BrokerChannels.SCORES_UPDATE
    gen = await broker.subscribe(game_id, channel)
    queue_set = broker._subscribers[game_id][channel]
    assert len(queue_set) == 1

    messages = []

    async def consume() -> None:
        async for message in gen:
            messages.append(message)

    task = asyncio.create_task(consume())
    await broker.shutdown()
    await task

    assert game_id not in broker._subscribers or channel not in broker._subscribers[game_id]


@pytest.mark.asyncio
async def test_shutdown_sends_sentinel_and_clears_state(
    broker: InMemoryMessageBroker,
) -> None:
    game_id: str = "shutdown-game"
    channel: BrokerChannels = BrokerChannels.SCORES_UPDATE

    # Subscribe to the broker and get the generator
    message_generator = await broker.subscribe(game_id, channel)

    # Set up an additional listener to simulate another consumer
    listening_queue: asyncio.Queue[Any] = asyncio.Queue(maxsize=1)
    broker._subscribers[game_id][channel][listening_queue] = OverflowPolicy.BLOCK

    # Simulate shutdown
    await broker.shutdown()

    # Sentinel message should have been sent to all queues
    sentinel: dict[str, Any] = listening_queue.get_nowait()
    assert sentinel.get("__sentinel__") is True

    # Consume the generator to allow it to exit
    async def consume_messages() -> None:
        async for _ in message_generator:
            pass  # All messages (if any) are ignored in this test

    consumer_task: asyncio.Task[None] = asyncio.create_task(consume_messages())
    await consumer_task

    # Broker's internal state should now be empty
    assert broker._subscribers == {}


@pytest.mark.asyncio
async def test_publish_after_shutdown_is_ignored(
    broker: InMemoryMessageBroker,
) -> None:
    await broker.shutdown()
    count = await broker.publish("any-game", BrokerChannels.SCORES_UPDATE, {"x": 1})
    assert count == 0


@pytest.mark.asyncio
async def test_drop_old_policy_keeps_freshest(broker: InMemoryMessageBroker) -> None:
    game_id = "overflow"
    gen = await broker.subscribe(
        game_id,
        BrokerChannels.SCORES_UPDATE,
        policy=OverflowPolicy.DROP_OLD,
        maxsize=2,
    )

    for i in range(5):
        await broker.publish(game_id, BrokerChannels.SCORES_UPDATE, {"i": i})

    received = [await anext(gen), await anext(gen)]
    assert received == [{"i": 3}, {"i": 4}]
    assert broker._dropped_messages == 3


@pytest.mark.asyncio
async def test_sentinel_always_delivered_when_full(broker: InMemoryMessageBroker) -> None:
    game_id = "sentinel-full"
    gen = await broker.subscribe(
        game_id,
        BrokerChannels.SCORES_UPDATE,
        policy=OverflowPolicy.DROP_NEW,
        maxsize=1,
    )

    await broker.publish(game_id, BrokerChannels.SCORES_UPDATE, {"i": 0})
    await broker.publish(game_id, BrokerChannels.SCORES_UPDATE, {"i": 1})  # dropped

    await broker.publish(game_id, BrokerChannels.SCORES_UPDATE, {"__sentinel__": True})

    # The sentinel terminates the consumer even though the queue was saturated.
    with pytest.raises(StopAsyncIteration):
        await asyncio.wait_for(anext(gen), timeout=2)
