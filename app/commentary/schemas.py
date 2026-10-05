from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

from app.shared.enums.game_event import GameEvent


class CommentaryDraft(BaseModel):
    """A provider's raw output before it is enriched into a full line."""

    text: str
    emotion: str = "neutral"


class CommentaryLine(BaseModel):
    """A fully formed commentary line ready to be delivered to clients."""

    game_id: str
    point_index: int
    text: str
    emotion: str = "neutral"
    importance: float = 0.0
    tags: list[str] = Field(default_factory=list)
    source: str = "template"
    ts: float = 0.0

    def to_message(self) -> dict[str, Any]:
        """Wrap the line in the ``{type, data}`` broker/socket envelope."""
        return {
            "type": GameEvent.GAME_COMMENTARY.value,
            "data": self.model_dump(),
        }
