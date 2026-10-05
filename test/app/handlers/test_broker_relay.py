from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.handlers.broker_relay import BrokerRelay
from app.shared.enums.broker_channels import BrokerChannels


@pytest.fixture
def mock_context() -> MagicMock:
    """Provides a mock AppContext."""
    context = MagicMock()
    context.logger = MagicMock()
    context.broker = AsyncMock()
    context.sio = AsyncMock()
    return context


@pytest.fixture
def broker_relay(mock_context: MagicMock) -> BrokerRelay:
    """Provides a BrokerRelay instance with a mocked context."""
    return BrokerRelay(mock_context.sio, mock_context.broker, mock_context.logger)


@pytest.mark.asyncio
async def test_start_listener_creates_task_on_first_call(broker_relay: BrokerRelay, mock_context: MagicMock) -> None:
    """Verify that start_listener creates and stores a task on the first call."""
    game_id = "game1"
    channels = [BrokerChannels.SCORES_UPDATE]
    namespace = "/game"
    processor = AsyncMock()

    async def empty_generator() -> AsyncGenerator[Any, None]:
        if False:
            yield

    mock_context.broker.subscribe.return_value = empty_generator()

    key = broker_relay._create_subscription_key(game_id, channels)
    assert key not in broker_relay._tasks

    await broker_relay.start_listener(game_id, channels, namespace, processor)

    assert key in broker_relay._tasks
    task = broker_relay._tasks[key]
    assert isinstance(task, asyncio.Task)
    assert not task.done()

    # Cleanup
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.asyncio
async def test_start_listener_reuses_existing_task(broker_relay: BrokerRelay, mock_context: MagicMock) -> None:
    """Verify that subsequent calls to start_listener reuse an existing task."""
    game_id = "game1"
    channels = [BrokerChannels.SCORES_UPDATE]
    namespace = "/game"
    processor = AsyncMock()

    async def empty_generator() -> AsyncGenerator[Any, None]:
        if False:
            yield

    mock_context.broker.subscribe.return_value = empty_generator()

    # First call
    await broker_relay.start_listener(game_id, channels, namespace, processor)
    key = broker_relay._create_subscription_key(game_id, channels)
    assert len(broker_relay._tasks) == 1
    task1 = broker_relay._tasks[key]

    # Second call
    await broker_relay.start_listener(game_id, channels, namespace, processor)
    assert len(broker_relay._tasks) == 1
    task2 = broker_relay._tasks[key]

    assert task1 is task2

    # Cleanup
    task1.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task1


@pytest.mark.asyncio
async def test_listener_processes_and_emits_messages(broker_relay: BrokerRelay, mock_context: MagicMock) -> None:
    """Test the full loop: subscribe, process, and emit."""
    game_id = "game1"
    channels = [BrokerChannels.SCORES_UPDATE]
    namespace = "/game"

    stop_event = asyncio.Event()

    async def message_generator() -> AsyncGenerator[Any, None]:
        yield {"type": "score", "data": 1}
        yield {"type": "score", "data": 2}
        await stop_event.wait()  # Block to keep the task alive

    mock_context.broker.subscribe.return_value = message_generator()

    processor = AsyncMock(return_value=("event_name", {"payload": "data"}))

    task = asyncio.create_task(broker_relay._listener(game_id, channels, namespace, processor))

    await asyncio.sleep(0.01)  # Allow the listener to process

    assert processor.call_count == 2
    processor.assert_any_call({"type": "score", "data": 1})
    processor.assert_any_call({"type": "score", "data": 2})

    assert mock_context.sio.emit.call_count == 2
    mock_context.sio.emit.assert_called_with("event_name", {"payload": "data"}, room=game_id, namespace=namespace)

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.asyncio
async def test_done_callback_removes_task(broker_relay: BrokerRelay, mock_context: MagicMock) -> None:
    """Verify that the done_callback removes the task from the registry."""
    game_id = "game1"
    channels = [BrokerChannels.SCORES_UPDATE]
    key = broker_relay._create_subscription_key(game_id, channels)

    async def finite_generator() -> AsyncGenerator[Any, None]:
        yield {"__sentinel__": True}

    mock_context.broker.subscribe.return_value = finite_generator()

    await broker_relay.start_listener(game_id, channels, "/game", AsyncMock())

    assert key in broker_relay._tasks
    task = broker_relay._tasks[key]

    await task  # Wait for the task to finish naturally

    assert key not in broker_relay._tasks


@pytest.mark.asyncio
async def test_shutdown_cancels_all_tasks(broker_relay: BrokerRelay, mock_context: MagicMock) -> None:
    """Verify that shutdown cancels all running listener tasks."""

    # Start two different listeners
    await broker_relay.start_listener("game1", [BrokerChannels.SCORES_UPDATE], "/g", AsyncMock())
    await broker_relay.start_listener("game2", [BrokerChannels.CONTROLS], "/g", AsyncMock())

    assert len(broker_relay._tasks) == 2
    tasks = list(broker_relay._tasks.values())

    await broker_relay.shutdown()

    assert len(broker_relay._tasks) == 0
    assert all(t.cancelled() for t in tasks)


@pytest.mark.asyncio
async def test_stop_for_game_cancels_only_matching_game(broker_relay: BrokerRelay, mock_context: MagicMock) -> None:
    """Verify that stop_for_game releases one game's tasks and keeps others."""

    block = asyncio.Event()

    async def _blocking_generator() -> AsyncGenerator[Any, None]:
        await block.wait()
        if False:
            yield

    mock_context.broker.subscribe.side_effect = lambda *args, **kwargs: _blocking_generator()

    await broker_relay.start_listener("game1", [BrokerChannels.SCORES_UPDATE], "/g", AsyncMock())
    await broker_relay.start_listener("game1", [BrokerChannels.CONTROLS], "/g", AsyncMock())
    await broker_relay.start_listener("game2", [BrokerChannels.CONTROLS], "/g", AsyncMock())
    assert len(broker_relay._tasks) == 3

    game2_key = broker_relay._create_subscription_key("game2", [BrokerChannels.CONTROLS])
    game2_task = broker_relay._tasks[game2_key]

    await broker_relay.stop_for_game("game1")

    assert len(broker_relay._tasks) == 1
    assert game2_key in broker_relay._tasks
    assert not game2_task.done()

    await broker_relay.shutdown()
