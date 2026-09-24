from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncGenerator
from configparser import ConfigParser
from contextlib import suppress
from typing import Any

from app.broker.message_broker import MessageBroker, OverflowPolicy
from app.commentary.agent import CommentaryAgent
from app.commentary.events import CommentaryEvent
from app.commentary.providers.factory import create_commentary_provider
from app.commentary.salience import importance_of
from app.commentary.store import CommentaryStore
from app.shared.enums.broker_channels import BrokerChannels
from gameengine.store.game_data import GameMetaData
from utils.logger import get_logger


class CommentaryWorker:
    """
    Per-game background task that consumes score updates and produces commentary.

    Decoupled from the scheduler: it subscribes to the same broker channel the
    relay uses. Ingest is separated from generation and coalesced through a
    small, importance-prioritised buffer, so slow (e.g. LLM) generation can never
    back up the broker or delay the game loop. Game metadata is supplied by the
    scheduler layer at construction.
    """

    def __init__(
        self,
        *,
        game_id: str,
        broker: MessageBroker,
        config: ConfigParser,
        game_details: GameMetaData | None = None,
        store: CommentaryStore | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self.game_id = game_id
        self._broker = broker
        self._config = config
        self._game_details = game_details
        self._store = store
        self.logger = logger or get_logger(self.__class__.__name__)
        self._capacity = max(1, config.getint("commentary", "pendingBuffer", fallback=3))

        self._pending: list[tuple[float, int, CommentaryEvent]] = []
        self._sequence = 0
        self._wakeup = asyncio.Event()
        self._ingest_done = False

        self._task: asyncio.Task[None] | None = None
        self._generation_task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        if self._task is not None and not self._task.done():
            return
        # Subscribe before spawning the task so no score updates are missed
        # between scheduler start and worker readiness. The worker drains fast,
        # so saturation is unlikely; DROP_OLD keeps it current if it ever happens.
        iterator = await self._broker.subscribe(
            self.game_id,
            BrokerChannels.SCORES_UPDATE,
            policy=OverflowPolicy.DROP_OLD,
        )
        self._task = asyncio.create_task(self._run(iterator), name=f"commentary_{self.game_id}")

    async def stop(self) -> None:
        task = self._task
        generation = self._generation_task
        self._task = None
        self._generation_task = None

        for current in (generation, task):
            if current is not None and not current.done():
                current.cancel()

        with suppress(asyncio.CancelledError):
            if task is not None:
                await task
        with suppress(asyncio.CancelledError):
            if generation is not None:
                await generation

    def _enqueue_event(self, event: CommentaryEvent) -> None:
        """Add to the bounded buffer, evicting the least important pending item."""
        self._sequence += 1
        self._pending.append((importance_of(event), self._sequence, event))
        if len(self._pending) > self._capacity:
            # Lowest importance first; tie-break on oldest sequence.
            index = min(range(len(self._pending)), key=lambda i: (self._pending[i][0], self._pending[i][1]))
            self._pending.pop(index)
        self._wakeup.set()

    def _dequeue_event(self) -> CommentaryEvent | None:
        if not self._pending:
            return None
        # Highest importance first; tie-break on newest sequence.
        index = max(range(len(self._pending)), key=lambda i: (self._pending[i][0], -self._pending[i][1]))
        return self._pending.pop(index)[2]

    async def _generate_loop(self, agent: CommentaryAgent) -> None:
        while True:
            event = self._dequeue_event()
            if event is not None:
                try:
                    await agent.process_event(event)
                except Exception:
                    self.logger.exception("Commentary generation failed for game %s", self.game_id)
                continue

            if self._ingest_done:
                return

            self._wakeup.clear()
            if not self._pending and not self._ingest_done:
                await self._wakeup.wait()

    async def _run(self, iterator: AsyncGenerator[Any, None]) -> None:
        provider = create_commentary_provider(self._config, self.logger)
        agent = CommentaryAgent(
            game_id=self.game_id,
            provider=provider,
            broker=self._broker,
            config=self._config,
            game_details=self._game_details,
            store=self._store,
            logger=self.logger,
        )
        await agent.prime()

        self.logger.info(
            "Commentary worker started for game %s (provider=%s)",
            self.game_id,
            provider.source,
        )

        self._generation_task = asyncio.create_task(
            self._generate_loop(agent),
            name=f"commentary_gen_{self.game_id}",
        )

        try:
            async for message in iterator:
                if not isinstance(message, dict):
                    continue
                if message.get("__sentinel__"):
                    break
                try:
                    event = agent.build_event(message)
                except Exception:
                    self.logger.exception("Failed to build commentary event for game %s", self.game_id)
                    continue
                self._enqueue_event(event)
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger.exception("Commentary worker error for game %s", self.game_id)
        finally:
            self._ingest_done = True
            self._wakeup.set()

            generation = self._generation_task
            if generation is not None and not generation.done():
                with suppress(asyncio.CancelledError, Exception):
                    await generation

            aclose = getattr(provider, "aclose", None)
            if aclose is not None:
                try:
                    await aclose()
                except Exception:
                    self.logger.debug("Failed to close commentary provider", exc_info=True)
            self.logger.info("Commentary worker stopped for game %s", self.game_id)
