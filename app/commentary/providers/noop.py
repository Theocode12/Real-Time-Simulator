from __future__ import annotations

from app.commentary.context import CommentaryContext
from app.commentary.events import CommentaryEvent
from app.commentary.schemas import CommentaryDraft


class NoopCommentaryProvider:
    """Provider that never emits commentary (used to disable the pipeline)."""

    source = "noop"

    async def generate(
        self,
        event: CommentaryEvent,
        context: CommentaryContext | None = None,
    ) -> CommentaryDraft | None:
        return None
