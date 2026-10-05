from __future__ import annotations

from typing import Protocol

from app.commentary.context import CommentaryContext
from app.commentary.events import CommentaryEvent
from app.commentary.schemas import CommentaryDraft


class CommentaryProvider(Protocol):
    """
    Strategy for turning a normalized event into a commentary draft.

    Implementations must be safe to call from the game's async context and
    should return ``None`` when they choose not to commentate an event. The
    optional ``context`` carries bounded rolling match memory.
    """

    source: str

    async def generate(
        self,
        event: CommentaryEvent,
        context: CommentaryContext | None = None,
    ) -> CommentaryDraft | None: ...
