from __future__ import annotations

import logging

from app.exceptions.support_error import SupportGameNotLiveError
from app.infra.support_store import SupportStore
from app.models.support import SupportCounts
from app.shared.enums.support import SupportSide
from utils.logger import get_logger


class SupportService:
    """
    Application service for the support (rooting) feature.

    Owns the business rules: a game must be live to cast a vote and the
    chosen side must be valid. Data access is delegated to ``SupportStore``.
    """

    def __init__(self, store: SupportStore, logger: logging.Logger | None = None) -> None:
        self.store = store
        self.logger = logger or get_logger(self.__class__.__name__)

    async def is_game_live(self, game_id: str) -> bool:
        return await self.store.is_game_live(game_id)

    async def get_counts(self, game_id: str) -> SupportCounts:
        counts = await self.store.get_counts(game_id)
        return SupportCounts.from_raw(counts)

    async def cast_vote(self, game_id: str, voter_id: str, side: SupportSide) -> SupportCounts:
        if not await self.store.is_game_live(game_id):
            raise SupportGameNotLiveError(game_id)

        counts = await self.store.cast_vote(game_id, voter_id, side)
        self.logger.info(
            "Support vote recorded: game=%s voter=%s side=%s counts=%s",
            game_id,
            voter_id,
            side.value,
            counts,
        )
        return SupportCounts.from_raw(counts)
