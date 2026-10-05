from __future__ import annotations

import asyncio
import logging
from configparser import ConfigParser
from contextlib import suppress
from unittest.mock import AsyncMock, MagicMock

import pytest
from pytest import MonkeyPatch

from app.scheduler.manager import SchedulerContext, SchedulerManager
from gameengine.store.game_data import GameMetaData


# Dummy GameScheduler and Feeder
class DummyFeeder:
    async def get_metadata(self) -> dict[str, bool]:
        return {"metadata": True}


class DummyScheduler:
    def __init__(
        self,
        game_id: str,
        broker: MagicMock,
        feeder: DummyFeeder,
        state_publisher: MagicMock,
    ) -> None:
        self.game_id = game_id
        self.broker = broker
        self.feeder = feeder
        self._stop_event = asyncio.Event()
        self.state_publisher = state_publisher

    async def run(self) -> None:
        await self._stop_event.wait()

    def stop(self) -> None:
        self._stop_event.set()

    async def get_metadata(self) -> dict[str, bool]:
        return {"metadata": True}


@pytest.fixture
def broker() -> MagicMock:
    return MagicMock()


@pytest.fixture
def scheduler_manager(broker: MagicMock, valid_config: ConfigParser, dummy_logger: logging.Logger) -> SchedulerManager:
    return SchedulerManager(broker, config=valid_config, logger=dummy_logger)


@pytest.mark.asyncio
async def test_shutdown_all(monkeypatch: MonkeyPatch, scheduler_manager: SchedulerManager) -> None:
    monkeypatch.setattr("app.scheduler.manager.create_game_feeder", lambda *a, **kw: DummyFeeder())
    monkeypatch.setattr("app.scheduler.manager.GameScheduler", DummyScheduler)

    await scheduler_manager.create_or_get_scheduler(SchedulerContext(game_id="game-a"))
    await scheduler_manager.create_or_get_scheduler(SchedulerContext(game_id="game-b"))

    await scheduler_manager.shutdown()

    assert scheduler_manager._schedulers == {}
    assert scheduler_manager._scheduler_tasks == {}


@pytest.mark.asyncio
async def test_create_and_get_scheduler(
    monkeypatch: MonkeyPatch,
    scheduler_manager: SchedulerManager,
) -> None:
    monkeypatch.setattr("app.scheduler.manager.create_game_feeder", lambda *a, **kw: DummyFeeder())
    monkeypatch.setattr("app.scheduler.manager.GameScheduler", DummyScheduler)

    game_id = "game-1"
    scheduler, task = await scheduler_manager.create_or_get_scheduler(SchedulerContext(game_id=game_id))

    assert scheduler_manager.has_scheduler(game_id)
    assert scheduler_manager.get_scheduler(game_id) is scheduler
    assert isinstance(task, asyncio.Task)

    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


@pytest.mark.asyncio
async def test_scheduler_reuse(monkeypatch: MonkeyPatch, scheduler_manager: SchedulerManager) -> None:
    monkeypatch.setattr("app.scheduler.manager.create_game_feeder", lambda *a, **kw: DummyFeeder())
    monkeypatch.setattr("app.scheduler.manager.GameScheduler", DummyScheduler)

    game_id = "game-2"
    sched1, task1 = await scheduler_manager.create_or_get_scheduler(SchedulerContext(game_id=game_id))
    sched2, task2 = await scheduler_manager.create_or_get_scheduler(SchedulerContext(game_id=game_id))

    assert sched1 is sched2
    assert task1 is task2

    task1.cancel()

    try:
        await task1
    except asyncio.CancelledError:
        pass


@pytest.mark.asyncio
async def test_get_game_data(monkeypatch: MonkeyPatch, scheduler_manager: SchedulerManager) -> None:
    monkeypatch.setattr("app.scheduler.manager.create_game_feeder", lambda *a, **kw: DummyFeeder())
    monkeypatch.setattr("app.scheduler.manager.GameScheduler", DummyScheduler)

    game_id = "game-3"
    _, task = await scheduler_manager.create_or_get_scheduler(SchedulerContext(game_id=game_id))

    metadata = await scheduler_manager.get_game_data(game_id)
    assert metadata == {"metadata": True}

    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


@pytest.mark.asyncio
async def test_cleanup_scheduler(monkeypatch: MonkeyPatch, scheduler_manager: SchedulerManager) -> None:
    monkeypatch.setattr("app.scheduler.manager.create_game_feeder", lambda *a, **kw: DummyFeeder())
    monkeypatch.setattr("app.scheduler.manager.GameScheduler", DummyScheduler)

    game_id = "game-4"
    scheduler, task = await scheduler_manager.create_or_get_scheduler(SchedulerContext(game_id=game_id))

    assert scheduler_manager.has_scheduler(game_id)

    scheduler.stop()  # type: ignore

    await asyncio.sleep(0.05)  # Yield control to let task finish and cleanup trigger

    await task
    assert task.done()

    if scheduler_manager._background_tasks:
        await scheduler_manager._background_tasks.pop()

    assert not scheduler_manager.has_scheduler(game_id)


@pytest.mark.asyncio
async def test_create_scheduler_forwards_game_details_to_commentary(
    monkeypatch: MonkeyPatch,
    valid_config: ConfigParser,
    dummy_logger: logging.Logger,
) -> None:
    details: dict = {
        "game_id": "game-c",
        "teams": {
            "team_1": {"name": "A", "players": [{"name": "A"}]},
            "team_2": {"name": "B", "players": [{"name": "B"}]},
        },
    }

    class FeederWithDetails(DummyFeeder):
        async def get_game_details(self) -> dict:
            return details

    commentary = MagicMock()
    commentary.start_for_game = AsyncMock()
    commentary.stop_for_game = AsyncMock()

    scheduler_manager = SchedulerManager(
        MagicMock(),
        config=valid_config,
        logger=dummy_logger,
        commentary_manager=commentary,
    )

    monkeypatch.setattr("app.scheduler.manager.create_game_feeder", lambda *a, **kw: FeederWithDetails())
    monkeypatch.setattr("app.scheduler.manager.GameScheduler", DummyScheduler)

    _, task = await scheduler_manager.create_or_get_scheduler(SchedulerContext(game_id="game-c"))

    commentary.start_for_game.assert_awaited_once()
    forward_args = commentary.start_for_game.await_args.args
    assert forward_args[0] == "game-c"
    forwarded = forward_args[1]
    assert isinstance(forwarded, GameMetaData)
    assert forwarded.teams["team_1"].name == "A"

    task.cancel()
    with suppress(asyncio.CancelledError):
        await task

    for _ in range(3):
        await asyncio.sleep(0)
        pending = list(scheduler_manager._background_tasks)
        if not pending:
            break
        await asyncio.gather(*pending, return_exceptions=True)
