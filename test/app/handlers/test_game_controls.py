from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from app.exceptions.message_error import MessageError
from app.handlers.game_controls import GameControlHandler, SpeedControlHandler, SpeedControlSchema
from app.shared.enums.broker_channels import BrokerChannels
from app.shared.enums.game_event import GameEvent
from app.websockets_api.namespaces.message_dispacter import MessageDispatcher


@pytest.fixture
def mock_context() -> MagicMock:
    """Provides a mock AppContext for handler tests."""
    context = MagicMock()
    context.auth = MagicMock()
    context.scheduler_manager = MagicMock()
    context.sio = AsyncMock()
    context.broker = AsyncMock()
    context.logger = MagicMock()
    return context


@pytest.fixture
def game_control_handler(mock_context: MagicMock) -> GameControlHandler:
    """Provides a GameControlHandler instance with a mocked context."""
    return GameControlHandler(mock_context)


@pytest.mark.asyncio
async def test_handle_unauthenticated_request(
    game_control_handler: GameControlHandler, mock_context: MagicMock
) -> None:
    """Verify that an unauthenticated request is rejected."""
    # Arrange
    mock_context.auth.validate.return_value = False
    mock_context.auth.validate_for_game.return_value = False
    sid = "test_sid"
    data = {"token": "invalid_token", "game_id": "game1", "namespace": "/game"}

    # Act
    await game_control_handler.handle(sid, data)

    # Assert
    mock_context.auth.validate.assert_called_once_with("invalid_token")
    mock_context.sio.emit.assert_awaited_once_with(
        GameEvent.ERROR, {"error": "Unauthorized"}, to=sid, namespace="/game"
    )
    mock_context.broker.publish.assert_not_awaited()


@pytest.mark.asyncio
async def test_handle_game_not_found(game_control_handler: GameControlHandler, mock_context: MagicMock) -> None:
    """Verify that a request for a non-existent game is rejected."""
    # Arrange
    mock_context.auth.validate.return_value = True
    mock_context.auth.validate_for_game.return_value = True
    mock_context.scheduler_manager.has_scheduler.return_value = False
    sid = "test_sid"
    game_id = "non_existent_game"
    namespace = "/game"
    data = {"token": "valid_token", "game_id": game_id, "namespace": namespace}

    # Act
    await game_control_handler.handle(sid, data)

    # Assert
    mock_context.auth.validate.assert_called_once_with("valid_token")
    mock_context.auth.validate_for_game.assert_called_once_with("valid_token", game_id)
    mock_context.scheduler_manager.has_scheduler.assert_called_once_with(game_id)
    mock_context.sio.emit.assert_awaited_once_with(
        GameEvent.ERROR,
        {"error": "Game not found or not running"},
        to=sid,
        namespace=namespace,
    )
    mock_context.broker.publish.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "control_type",
    [GameEvent.GAME_CONTROL_START, GameEvent.GAME_CONTROL_PAUSE, GameEvent.GAME_CONTROL_RESUME],
)
async def test_handle_success_publishes_control_message(
    control_type: GameEvent,
    game_control_handler: GameControlHandler,
    mock_context: MagicMock,
) -> None:
    """Verify a valid request publishes a message to the broker."""
    # Arrange
    mock_context.auth.validate.return_value = True
    mock_context.auth.validate_for_game.return_value = True
    mock_context.scheduler_manager.has_scheduler.return_value = True
    sid = "test_sid"
    game_id = "active_game"
    namespace = "/game"
    data = {
        "token": "valid_token",
        "game_id": game_id,
        "type": control_type,
        "namespace": namespace,
    }

    # Act
    await game_control_handler.handle(sid, data)

    # Assert
    mock_context.auth.validate.assert_called_once_with("valid_token")
    mock_context.auth.validate_for_game.assert_called_once_with("valid_token", game_id)
    mock_context.scheduler_manager.has_scheduler.assert_called_once_with(game_id)
    mock_context.sio.emit.assert_not_awaited()

    # Verify the token was removed from the payload before publishing
    expected_payload = {
        "game_id": game_id,
        "type": control_type,
        "namespace": namespace,
    }
    mock_context.broker.publish.assert_awaited_once_with(game_id, BrokerChannels.CONTROLS, expected_payload)


@pytest.mark.asyncio
@pytest.mark.parametrize("speed", [0, 8, "fast", "4", True])
async def test_dispatch_rejects_invalid_speed(speed: object, mock_context: MagicMock) -> None:
    mock_context.router.get_definition.return_value = {
        "schema": SpeedControlSchema,
        "handler": GameControlHandler,
    }

    with pytest.raises(MessageError, match="Invalid data schema"):
        await MessageDispatcher(mock_context).dispatch(
            "test_sid",
            {"game_id": "game1", "token": "token", "type": GameEvent.GAME_CONTROL_SPEED, "speed": speed},
            "/game",
        )


@pytest.mark.asyncio
async def test_wrong_game_token_is_rejected_with_game_namespace(mock_context: MagicMock) -> None:
    from app.core.ws_auth import AuthService

    mock_context.auth = AuthService(secret="test-secret")
    mock_context.scheduler_manager.has_scheduler.return_value = True
    handler = GameControlHandler(mock_context)
    token = mock_context.auth.issue_token("game-owner")

    await handler.handle(
        "test_sid",
        {"token": token, "game_id": "different-game", "namespace": "/game"},
    )

    mock_context.sio.emit.assert_awaited_once_with(
        GameEvent.ERROR,
        {"error": "Unauthorized"},
        to="test_sid",
        namespace="/game",
    )
    mock_context.broker.publish.assert_not_awaited()


@pytest.mark.asyncio
async def test_expired_token_is_rejected_with_game_namespace(mock_context: MagicMock, monkeypatch: pytest.MonkeyPatch) -> None:
    from app.core.ws_auth import AuthService

    auth = AuthService(secret="test-secret", token_ttl_seconds=1)
    monkeypatch.setattr("app.core.ws_auth.time.time", lambda: 100)
    token = auth.issue_token("game-a")
    monkeypatch.setattr("app.core.ws_auth.time.time", lambda: 101)
    mock_context.auth = auth

    await GameControlHandler(mock_context).handle(
        "test_sid",
        {"token": token, "game_id": "game-a", "namespace": "/game"},
    )

    mock_context.sio.emit.assert_awaited_once_with(
        GameEvent.ERROR,
        {"error": "Unauthorized"},
        to="test_sid",
        namespace="/game",
    )
    mock_context.broker.publish.assert_not_awaited()


@pytest.mark.asyncio
async def test_valid_token_publishes_stripped_speed_control(mock_context: MagicMock) -> None:
    from app.core.ws_auth import AuthService

    mock_context.auth = AuthService(secret="test-secret")
    mock_context.scheduler_manager.has_scheduler.return_value = True
    token = mock_context.auth.issue_token("game-a")

    await SpeedControlHandler(mock_context).handle(
        "test_sid",
        {
            "token": token,
            "game_id": "game-a",
            "type": GameEvent.GAME_CONTROL_SPEED,
            "speed": 4,
            "namespace": "/game",
        },
    )

    mock_context.broker.publish.assert_awaited_once_with(
        "game-a",
        BrokerChannels.CONTROLS,
        {"game_id": "game-a", "type": GameEvent.GAME_CONTROL_SPEED, "speed": 4, "namespace": "/game"},
    )
