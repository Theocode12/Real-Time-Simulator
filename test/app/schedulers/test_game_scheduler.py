from __future__ import annotations

import asyncio
from asyncio import Task
from collections.abc import AsyncGenerator
from configparser import ConfigParser
from logging import Logger
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.scheduler.scheduler import GameScheduler, SchedulerCommands, SchedulerState
from app.shared.enums.game_event import GameEvent


@pytest.fixture
def dummy_feeder() -> MagicMock:
    feeder = MagicMock()
    feeder.get_game_details = AsyncMock(return_value={"teams": ["A", "B"]})

    async def dummy_scores() -> AsyncGenerator[Any, Any]:
        for i in range(3):
            yield {"score_update": i}
            await asyncio.sleep(0.01)

    feeder.get_next_score = lambda: dummy_scores()
    feeder.cleanup = AsyncMock()
    return feeder


@pytest.fixture
def dummy_broker() -> MagicMock:
    broker = MagicMock()
    broker.subscribe = AsyncMock()

    async def dummy_control_messages() -> AsyncGenerator[Any, Any]:
        yield {"type": SchedulerCommands.START}
        yield {"type": SchedulerCommands.PAUSE}
        yield {"type": SchedulerCommands.RESUME}
        yield {"type": SchedulerCommands.ADJUST_SPEED, "speed": 2.5}
        yield {"type": "UNKNOWN_COMMAND"}

    broker.subscribe.return_value = dummy_control_messages()
    return broker


@pytest.mark.asyncio
async def test_start_sets_state_and_unblocks_pause_event(
    valid_config: ConfigParser,
    dummy_logger: Logger,
    dummy_feeder: MagicMock,
    dummy_broker: MagicMock,
) -> None:
    scheduler = GameScheduler(
        game_id="test_game",
        broker=dummy_broker,
        feeder=dummy_feeder,
        config=valid_config,
        logger=dummy_logger,
    )
    await scheduler.start()
    assert scheduler.state == SchedulerState.ONGOING
    assert scheduler.pause_event.is_set()


@pytest.mark.asyncio
async def test_pause_sets_state_and_cancels_sleep(
    valid_config: ConfigParser,
    dummy_logger: Logger,
    dummy_feeder: MagicMock,
    dummy_broker: MagicMock,
) -> None:
    scheduler = GameScheduler(
        game_id="test_game",
        broker=dummy_broker,
        feeder=dummy_feeder,
        config=valid_config,
        logger=dummy_logger,
    )

    await scheduler.start()
    await scheduler.pause()

    assert scheduler.state == SchedulerState.PAUSED
    assert not scheduler.pause_event.is_set()

    scheduler._cancel_pause_timer()


@pytest.mark.asyncio
async def test_resume_sets_state_and_cancels_pause_timer(
    valid_config: ConfigParser,
    dummy_logger: Logger,
    dummy_feeder: MagicMock,
    dummy_broker: MagicMock,
) -> None:
    scheduler = GameScheduler(
        game_id="test_game",
        broker=dummy_broker,
        feeder=dummy_feeder,
        config=valid_config,
        logger=dummy_logger,
    )
    await scheduler.pause()
    assert isinstance(scheduler._pause_timer, Task)

    await scheduler.resume()

    assert scheduler.state == SchedulerState.ONGOING
    assert scheduler.pause_event.is_set()
    assert scheduler._pause_timer is None


@pytest.mark.asyncio
async def test_adjust_speed_changes_speed_and_cancels_sleep(
    valid_config: ConfigParser,
    dummy_logger: Logger,
    dummy_feeder: MagicMock,
    dummy_broker: MagicMock,
) -> None:
    scheduler = GameScheduler(
        game_id="test_game",
        broker=dummy_broker,
        feeder=dummy_feeder,
        config=valid_config,
        logger=dummy_logger,
    )
    scheduler.score_update_sleep_task = asyncio.create_task(asyncio.sleep(10))

    await scheduler.adjust_speed(2.0)
    assert scheduler.speed == 2.0
    await asyncio.sleep(0.01)
    assert scheduler.score_update_sleep_task.cancelled()


@pytest.mark.asyncio
async def test_adjust_speed_ignores_invalid_input(
    valid_config: ConfigParser,
    dummy_logger: Logger,
    dummy_feeder: MagicMock,
    dummy_broker: MagicMock,
) -> None:
    scheduler = GameScheduler(
        game_id="test_game",
        broker=dummy_broker,
        feeder=dummy_feeder,
        config=valid_config,
        logger=dummy_logger,
    )
    initial_speed = scheduler.speed
    await scheduler.adjust_speed(-5)
    assert scheduler.speed == initial_speed


@pytest.mark.asyncio
async def test_get_metadata_returns_data_combined_from_feeder_and_scheduler(
    valid_config: ConfigParser,
    dummy_logger: Logger,
    dummy_feeder: MagicMock,
    dummy_broker: MagicMock,
) -> None:
    scheduler = GameScheduler(
        game_id="test_game",
        broker=dummy_broker,
        feeder=dummy_feeder,
        config=valid_config,
        logger=dummy_logger,
    )
    scheduler.state = SchedulerState.ONGOING
    metadata = await scheduler.get_metadata()
    assert "game_state" in metadata
    assert "teams" in metadata
    assert metadata["game_state"] == SchedulerState.ONGOING


@pytest.mark.asyncio
async def test_control_subscription_routes_commands(
    valid_config: ConfigParser,
    dummy_logger: Logger,
    dummy_feeder: MagicMock,
    dummy_broker: MagicMock,
) -> None:
    scheduler = GameScheduler(
        game_id="game1",
        broker=dummy_broker,
        feeder=dummy_feeder,
        config=valid_config,
        logger=dummy_logger,
    )

    task = asyncio.create_task(scheduler.subscribe_to_controls())
    await asyncio.sleep(0.1)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    # Validate final state was updated by commands
    assert scheduler.state == SchedulerState.ONGOING
    assert scheduler.speed == 2.5


@pytest.mark.asyncio
async def test_resume_due_to_timeout_resumes_scheduler(
    valid_config: ConfigParser,
    dummy_logger: Logger,
    dummy_feeder: MagicMock,
    dummy_broker: MagicMock,
) -> None:
    scheduler = GameScheduler(
        game_id="test_game",
        broker=dummy_broker,
        feeder=dummy_feeder,
        config=valid_config,
        logger=dummy_logger,
    )

    scheduler.pause_timeout_secs = 0.01
    await scheduler.pause()
    scheduler.score_update_sleep_task = asyncio.create_task(asyncio.sleep(10))
    await asyncio.sleep(0.02)
    assert scheduler.state == SchedulerState.AUTOPLAY
    assert scheduler.pause_event.is_set()
    assert scheduler.score_update_sleep_task.cancelled()


def test_format_score_update_payload(
    valid_config: ConfigParser,
    dummy_logger: Logger,
    dummy_feeder: MagicMock,
    dummy_broker: MagicMock,
) -> None:
    scheduler = GameScheduler(
        game_id="test_game",
        broker=dummy_broker,
        feeder=dummy_feeder,
        config=valid_config,
        logger=dummy_logger,
    )

    raw_score = {"home": 1, "away": 2}
    dummy_feeder.consumed_count = 3
    expected = {
        "data": raw_score,
        "type": GameEvent.GAME_SCORE_UPDATE,
        "point_index": 3,
    }

    result = scheduler._format_score_update_payload(raw_score)
    assert result == expected


@pytest.mark.asyncio
async def test_run_loop_publishes_scores_and_cleans_up(
    valid_config: ConfigParser,
    dummy_logger: Logger,
    dummy_feeder: MagicMock,
) -> None:
    # Spy on publish
    publish_calls = []
    dummy_broker = AsyncMock()

    class TestScheduler(GameScheduler):
        from app.shared.enums.broker_channels import BrokerChannels

        async def publish(self, channel: BrokerChannels, message: Any) -> None:
            publish_calls.append((channel, message))

    scheduler = TestScheduler(
        "test_game",
        dummy_broker,
        dummy_feeder,
        config=valid_config,
        logger=dummy_logger,
    )
    # Start in unpaused state
    scheduler.speed = 0.05
    await scheduler.start()

    task = asyncio.create_task(scheduler.run())

    await asyncio.sleep(0.1)  # Let a few iterations complete
    task.cancel()

    try:
        await task
    except asyncio.CancelledError:
        pass

    # Assertions
    assert len(publish_calls) >= 1

    dummy_feeder.cleanup.assert_awaited()


@pytest.mark.asyncio
async def test_run_publishes_sentinel_on_completion(
    valid_config: ConfigParser,
    dummy_logger: Logger,
) -> None:
    """
    Verify that the GameScheduler publishes a sentinel message when the
    score feeder is exhausted.
    """
    # 1. Setup a feeder that will exhaust quickly
    feeder = MagicMock()
    feeder.get_game_details = AsyncMock(return_value={"teams": ["A", "B"]})

    async def finite_score_generator() -> AsyncGenerator[Any, Any]:
        yield {"score": 1}
        yield {"score": 2}

    feeder.get_next_score = finite_score_generator
    feeder.cleanup = AsyncMock()

    # 2. Setup a mock broker to spy on publish calls
    broker = AsyncMock()

    async def empty_generator() -> AsyncGenerator[Any, None]:
        if False:
            yield

    broker.subscribe.return_value = empty_generator()

    # 3. Create and run the scheduler
    scheduler = GameScheduler("test_game", broker, feeder, config=valid_config, logger=dummy_logger)
    scheduler.speed = 0  # Run as fast as possible
    await scheduler.start()  # Set state to ONGOING

    await scheduler.run()

    # 4. Assert the sentinel was the last message published
    broker.publish.assert_called()
    last_call = broker.publish.call_args_list[-1]
    _, channel, message = last_call.args

    assert channel == "scores_update"
    assert message == {"__sentinel__": True, "type": "end"}
