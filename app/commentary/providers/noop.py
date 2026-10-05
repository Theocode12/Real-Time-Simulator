from __future__ import annotations

from app.commentary.context import CommentaryContext
from app.commentary.events import CommentaryEvent
from app.commentary.schemas import CommentaryDraft


class NoopCommentaryProvider:
    """Provider that never emits commentary.

    Note: prefer disabling via ``[commentary] enabled=false`` (which skips
    workers, subscriptions and store creation entirely) over ``provider=noop``
    (which still runs the worker loop but emits nothing).
    """

    source = "noop"

    async def generate(
        self,
        event: CommentaryEvent,
        context: CommentaryContext | None = None,
    ) -> CommentaryDraft | None:
        return None
