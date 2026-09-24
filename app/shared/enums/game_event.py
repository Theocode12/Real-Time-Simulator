from __future__ import annotations

from enum import StrEnum, unique


@unique
class GameEvent(StrEnum):
    """All canonical event strings."""

    # Game
    GAME_JOIN = "game.join"
    GAME_LEAVE = "game.leave"
    GAME_START = "game.start"
    GAME_END = "game.end"

    # Game Controls
    GAME_CONTROL_START = "game.control.start"
    GAME_CONTROL_PAUSE = "game.control.pause"
    GAME_CONTROL_RESUME = "game.control.resume"
    GAME_CONTROL_SPEED = "game.control.speed"

    # Game Updates
    GAME_SCORE_UPDATE = "game.score.update"
    GAME_COMMENTARY = "game.commentary"

    # Error
    ERROR = "game.error"

    # Messages
    MESSAGE_JOIN = "message.join"
    MESSAGE_SEND = "message.send"

    # Viewer Count
    VIEWER_COUNT = "viewer.count"

    # Support (rooting for a side)
    SUPPORT_CAST = "support.cast"
    SUPPORT_UPDATE = "support.update"
