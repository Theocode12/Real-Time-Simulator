from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from app.exceptions.support_error import SupportGameNotLiveError
from app.models.support import SupportCounts
from app.shared.enums.game_event import GameEvent
from app.shared.enums.support import SupportSide
from app.websockets_api.namespaces.support_namespace import SupportNamespace


@pytest.fixture
def mock_context() -> MagicMock:
    context = MagicMock()
    context.sio = AsyncMock()
    context.logger = MagicMock()
    context.support = MagicMock()
    context.support.get_counts = AsyncMock(return_value=SupportCounts(team_1=1, team_2=2, total=3))
    context.support.cast_vote = AsyncMock(return_value=SupportCounts(team_1=2, team_2=2, total=4))
    return context


@pytest.fixture
def namespace(mock_context: MagicMock) -> SupportNamespace:
    ns = SupportNamespace("/support", mock_context)
    ns.emit = AsyncMock()
    ns.emit_error = AsyncMock()
    ns.enter_room = AsyncMock()
    ns.leave_room = AsyncMock()
    return ns


@pytest.mark.asyncio
async def test_on_connect(namespace: SupportNamespace) -> None:
    await namespace.on_connect("sid1", {})
    namespace.logger.debug.assert_called()


@pytest.mark.asyncio
async def test_on_join_enters_room_and_emits_counts(namespace: SupportNamespace, mock_context: MagicMock) -> None:
    await namespace.on_join("sid1", {"game_id": "g1"})

    namespace.enter_room.assert_awaited_once_with("sid1", "g1")
    mock_context.support.get_counts.assert_awaited_once_with("g1")
    namespace.emit.assert_awaited_once_with(
        GameEvent.SUPPORT_UPDATE,
        {"game_id": "g1", "team_1": 1, "team_2": 2, "total": 3},
        to="sid1",
    )


@pytest.mark.asyncio
async def test_on_join_invalid_payload(namespace: SupportNamespace) -> None:
    await namespace.on_join("sid1", {"voter_id": "v1"})

    namespace.emit_error.assert_awaited_once()
    namespace.enter_room.assert_not_awaited()


@pytest.mark.asyncio
async def test_on_cast_broadcasts_to_room(namespace: SupportNamespace, mock_context: MagicMock) -> None:
    await namespace.on_cast("sid1", {"game_id": "g1", "side": "team_1"})

    mock_context.support.cast_vote.assert_awaited_once_with("g1", "sid1", SupportSide.TEAM_1)
    namespace.enter_room.assert_awaited_once_with("sid1", "g1")
    namespace.emit.assert_awaited_once_with(
        GameEvent.SUPPORT_UPDATE,
        {"game_id": "g1", "team_1": 2, "team_2": 2, "total": 4},
        room="g1",
    )


@pytest.mark.asyncio
async def test_on_cast_prefers_voter_id(namespace: SupportNamespace, mock_context: MagicMock) -> None:
    await namespace.on_cast("sid1", {"game_id": "g1", "side": "team_2", "voter_id": "alice"})

    mock_context.support.cast_vote.assert_awaited_once_with("g1", "alice", SupportSide.TEAM_2)


@pytest.mark.asyncio
async def test_on_cast_invalid_side(namespace: SupportNamespace, mock_context: MagicMock) -> None:
    await namespace.on_cast("sid1", {"game_id": "g1", "side": "team_3"})

    namespace.emit_error.assert_awaited_once()
    mock_context.support.cast_vote.assert_not_awaited()


@pytest.mark.asyncio
async def test_on_cast_not_live_emits_error(namespace: SupportNamespace, mock_context: MagicMock) -> None:
    mock_context.support.cast_vote.side_effect = SupportGameNotLiveError("g1")

    await namespace.on_cast("sid1", {"game_id": "g1", "side": "team_1"})

    namespace.emit_error.assert_awaited_once()
    namespace.emit.assert_not_awaited()


@pytest.mark.asyncio
async def test_on_cast_throttles_rapid_repeats(namespace: SupportNamespace, mock_context: MagicMock) -> None:
    await namespace.on_cast("sid1", {"game_id": "g1", "side": "team_1"})
    await namespace.on_cast("sid1", {"game_id": "g1", "side": "team_2"})

    assert mock_context.support.cast_vote.await_count == 1


@pytest.mark.asyncio
async def test_on_disconnect_cleans_session(namespace: SupportNamespace) -> None:
    namespace._sessions["sid1"] = {"game_id": "g1", "voter_id": "v1"}

    await namespace.on_disconnect("sid1")

    assert "sid1" not in namespace._sessions
    namespace.leave_room.assert_awaited_once_with("sid1", "g1")
