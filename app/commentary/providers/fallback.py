from __future__ import annotations

from app.commentary.context import CommentaryContext
from app.commentary.events import CommentaryEvent
from app.commentary.providers.base import CommentaryProvider
from app.commentary.schemas import CommentaryDraft


class FallbackCommentaryProvider:
    """Tries the primary provider, then the fallback when it declines or fails."""

    def __init__(self, primary: CommentaryProvider, fallback: CommentaryProvider) -> None:
        self.primary = primary
        self.fallback = fallback
        self.source = getattr(primary, "source", "primary")

    async def generate(
        self,
        event: CommentaryEvent,
        context: CommentaryContext | None = None,
    ) -> CommentaryDraft | None:
        draft = await self.primary.generate(event, context)
        if draft is not None and draft.text:
            return draft
        return await self.fallback.generate(event, context)

    async def aclose(self) -> None:
        for provider in (self.primary, self.fallback):
            aclose = getattr(provider, "aclose", None)
            if aclose is not None:
                await aclose()
