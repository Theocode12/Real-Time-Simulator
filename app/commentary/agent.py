from __future__ import annotations

import logging
import time
from collections import deque
from configparser import ConfigParser
from dataclasses import replace
from typing import Any

from app.broker.message_broker import MessageBroker
from app.commentary.context import CommentaryContext
from app.commentary.events import CommentaryEvent, build_commentary_event
from app.commentary.formatting import point_summary
from app.commentary.providers.base import CommentaryProvider
from app.commentary.salience import MOMENTUM_STREAK, importance_of, should_commentate
from app.commentary.schemas import CommentaryLine
from app.commentary.store import CommentaryStore
from app.shared.enums.broker_channels import BrokerChannels
from gameengine.store.game_data import GameMetaData
from utils.logger import get_logger


class CommentaryAgent:
    """
    Stateful per-game commentary orchestrator.

    Owns the lightweight match memory (momentum, recent lines/points), applies
    the salience gate, delegates wording to a provider, persists the result, and
    publishes it on the commentary channel.
    """

    def __init__(
        self,
        *,
        game_id: str,
        provider: CommentaryProvider,
        broker: MessageBroker,
        config: ConfigParser,
        game_details: GameMetaData | None = None,
        store: CommentaryStore | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self.game_id = game_id
        self.provider = provider
        self.broker = broker
        self.config = config
        self.game_details = game_details
        self.store = store
        self.logger = logger or get_logger(self.__class__.__name__)

        self._threshold = config.getfloat("commentary", "salienceThreshold", fallback=0.3)
        self._routine_interval = config.getint("commentary", "routineInterval", fallback=2)
        self._recent_lines: deque[str] = deque(maxlen=config.getint("commentary", "recentLines", fallback=5))
        self._recent_points: deque[str] = deque(maxlen=config.getint("commentary", "recentPoints", fallback=6))
        self._seed_history = config.getboolean("commentary", "seedHistory", fallback=False)
        self._seed_history_limit = config.getint("commentary", "seedHistoryLimit", fallback=20)
        self._last_winner: str | None = None
        self._streak = 0

    async def prime(self) -> None:
        """Optionally seed rolling history from persisted commentary."""
        if not self._seed_history or self.store is None:
            return
        try:
            lines = await self.store.recent(self.game_id, limit=self._seed_history_limit)
        except Exception:
            self.logger.debug("Failed to seed commentary history for game %s", self.game_id, exc_info=True)
            return
        for line in lines:
            text = line.get("text")
            if isinstance(text, str) and text:
                self._recent_lines.append(text)

    def _apply_memory(self, event: CommentaryEvent) -> CommentaryEvent:
        winner = event.point.get("winner")
        winner = winner if isinstance(winner, str) else None

        if winner is not None and winner == self._last_winner:
            self._streak += 1
        else:
            self._streak = 1 if winner is not None else 0
        self._last_winner = winner

        if self._streak >= MOMENTUM_STREAK:
            tags = event.tags if "momentum" in event.tags else (*event.tags, "momentum")
            return replace(event, tags=tags, momentum_streak=self._streak)
        return event

    def build_event(self, message: dict[str, Any]) -> CommentaryEvent:
        """Normalize a score update into a commentary event (applies memory)."""
        return self._apply_memory(build_commentary_event(self.game_id, message, self.game_details))

    def _context(self) -> CommentaryContext:
        return CommentaryContext(
            recent_lines=tuple(self._recent_lines),
            recent_points=tuple(self._recent_points),
            momentum_side=self._last_winner if self._streak >= MOMENTUM_STREAK else None,
            streak=self._streak,
        )

    async def process_event(self, event: CommentaryEvent) -> CommentaryLine | None:
        """Apply the salience gate, generate, persist and publish a line."""
        context = self._context()

        line: CommentaryLine | None = None
        if should_commentate(event, threshold=self._threshold, routine_interval=self._routine_interval):
            try:
                draft = await self.provider.generate(event, context)
            except Exception:
                self.logger.exception("Commentary provider failed for game %s", self.game_id)
                draft = None

            if draft is not None and draft.text:
                line = CommentaryLine(
                    game_id=self.game_id,
                    point_index=event.point_index,
                    text=draft.text,
                    emotion=draft.emotion,
                    importance=importance_of(event),
                    tags=list(event.tags),
                    source=self.provider.source,
                    ts=time.time(),
                )
                self._recent_lines.append(line.text)

                if self.store is not None:
                    await self.store.append(self.game_id, line.model_dump())

                try:
                    await self.broker.publish(self.game_id, BrokerChannels.COMMENTARY, line.to_message())
                except Exception:
                    self.logger.exception("Failed to publish commentary for game %s", self.game_id)

        try:
            self._recent_points.append(point_summary(event))
        except Exception:
            self.logger.debug("Failed to summarize point for game %s", self.game_id, exc_info=True)

        return line

    async def handle_score(self, message: dict[str, Any]) -> CommentaryLine | None:
        """Process one score update; returns the emitted line, if any."""
        return await self.process_event(self.build_event(message))
