from __future__ import annotations


class SupportError(Exception):
    """Base class for support-feature errors."""


class SupportGameNotLiveError(SupportError):
    def __init__(self, game_id: str) -> None:
        super().__init__(f"Game '{game_id}' is not currently live.")
        self.game_id = game_id
