from __future__ import annotations

from configparser import ConfigParser
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.handlers.join_game import JoinGameHandler, _process_broker_message
from app.shared.enums.broker_channels import BrokerChannels
from app.shared.enums.game_event import GameEvent


@pytest.fixture
def mock_context() -> MagicMock:
    """Provides a mock AppContext for handler tests."""
    context = MagicMock()
    context.logger = MagicMock()
    context.scheduler_manager = MagicMock()
    context.scheduler_manager.get_game_data = AsyncMock()
    context.sio = AsyncMock()
    context.broker_relay = AsyncMock()
    context.commentary_manager = None

    config = ConfigParser()
    config.add_section("broker")
    config.set("broker", "relay_channels", "scores_update,controls")
    context.config = config

    return context


@pytest.fixture
def join_game_handler(mock_context: MagicMock) -> JoinGameHandler:
    """Provides a JoinGameHandler instance with a mocked context."""
    return JoinGameHandler(mock_context)


@pytest.mark.asyncio
async def test_handle_missing_game_id(join_game_handler: JoinGameHandler, mock_context: MagicMock) -> None:
    """Verify that a request without a game_id is rejected."""
    sid = "test_sid"
    data = {"namespace": "/game"}  # Missing game_id

    await join_game_handler.handle(sid, data)

    mock_context.sio.emit.assert_awaited_once_with(
        GameEvent.ERROR,
        {"error": "Missing required 'game_id' field."},
        to=sid,
        namespace="/game",
    )
    mock_context.scheduler_manager.has_scheduler.assert_not_called()


@pytest.mark.asyncio
async def test_handle_game_not_found(join_game_handler: JoinGameHandler, mock_context: MagicMock) -> None:
    """Verify that a request for a non-existent game is rejected."""
    sid = "test_sid"
    game_id = "non_existent_game"
    data = {"game_id": game_id, "namespace": "/game"}
    mock_context.scheduler_manager.has_scheduler.return_value = False

    await join_game_handler.handle(sid, data)

    mock_context.scheduler_manager.has_scheduler.assert_called_once_with(game_id)
    mock_context.sio.emit.assert_awaited_once_with(
        GameEvent.ERROR.value,
        {"error": f"Game '{game_id}' is not currently active or does not exist."},
        to=sid,
        namespace="/game",
    )


@pytest.mark.asyncio
async def test_handle_success(join_game_handler: JoinGameHandler, mock_context: MagicMock) -> None:
    """Verify a successful join request and response."""
    sid = "test_sid"
    game_id = "active_game"
    data = {"game_id": game_id, "namespace": "/game"}

    mock_context.scheduler_manager.has_scheduler.return_value = True
    mock_context.scheduler_manager.get_game_data.return_value = {"game_state": "ONGOING"}

    await join_game_handler.handle(sid, data)

    # Verify broker relay was started
    expected_channels = [BrokerChannels.SCORES_UPDATE, BrokerChannels.CONTROLS]
    mock_context.broker_relay.start_listener.assert_awaited_once_with(
        game_id, expected_channels, "/game", _process_broker_message
    )

    # Verify client was added to room
    mock_context.sio.enter_room.assert_awaited_once_with(sid, game_id, namespace="/game")

    # Verify response was sent
    mock_context.scheduler_manager.get_game_data.assert_awaited_once_with(game_id)
    mock_context.sio.emit.assert_awaited_once_with(
        GameEvent.GAME_JOIN,
        {
            "game_state": "ONGOING",
            "message": f"Successfully joined game {game_id}",
        },
        to=sid,
        namespace="/game",
    )


@pytest.mark.asyncio
async def test_handle_invalid_config_channels(join_game_handler: JoinGameHandler, mock_context: MagicMock) -> None:
    """Verify fallback to default channels if config is invalid."""
    sid = "test_sid"
    game_id = "active_game"
    data = {"game_id": game_id, "namespace": "/game"}

    mock_context.scheduler_manager.has_scheduler.return_value = True
    mock_context.scheduler_manager.get_game_data.return_value = {"game_state": "ONGOING"}
    mock_context.config.set("broker", "relay_channels", "scores_update,INVALID_CHANNEL")

    await join_game_handler.handle(sid, data)

    mock_context.logger.error.assert_called_once()
    expected_default_channels = [
        BrokerChannels.SCORES_UPDATE,
        BrokerChannels.CONTROLS,
    ]
    mock_context.broker_relay.start_listener.assert_awaited_once_with(
        game_id, expected_default_channels, "/game", _process_broker_message
    )


@pytest.mark.asyncio
async def test_handle_enter_room_failure(join_game_handler: JoinGameHandler, mock_context: MagicMock) -> None:
    """Verify error is emitted if entering the room fails."""
    sid = "test_sid"
    game_id = "active_game"
    data = {"game_id": game_id, "namespace": "/game"}

    mock_context.scheduler_manager.get_scheduler.return_value = AsyncMock()
    mock_context.sio.enter_room.side_effect = Exception("Connection error")

    await join_game_handler.handle(sid, data)

    mock_context.sio.emit.assert_awaited_once_with(
        GameEvent.ERROR,
        {"error": f"Failed to enter game room '{game_id}'."},
        to=sid,
        namespace="/game",
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "message, expected",
    [
        (
            {"type": GameEvent.GAME_SCORE_UPDATE, "data": "score"},
            (
                GameEvent.GAME_SCORE_UPDATE,
                {"type": GameEvent.GAME_SCORE_UPDATE, "data": "score"},
            ),
        ),
        (
            {"type": GameEvent.GAME_CONTROL_PAUSE, "data": "end"},
            (
                GameEvent.GAME_CONTROL_PAUSE,
                {"type": GameEvent.GAME_CONTROL_PAUSE, "data": "end"},
            ),
        ),
        (
            {"type": "game.score.update", "data": "score"},
            (
                GameEvent.GAME_SCORE_UPDATE,
                {"type": "game.score.update", "data": "score"},
            ),
        ),
        ({"data": "no type"}, None),
        ({"type": "invalid.event.type"}, None),
    ],
)
async def test_process_broker_message(message: dict[str, Any], expected: tuple[GameEvent, dict[str, Any]]) -> None:
    """Test the broker message processor utility function."""
    result = await _process_broker_message(message)
    assert result == expected
