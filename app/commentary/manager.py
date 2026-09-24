from __future__ import annotations

import asyncio
import logging
from configparser import ConfigParser
from typing import Any

from app.broker.message_broker import MessageBroker
from app.commentary.store import CommentaryStore
from app.commentary.worker import CommentaryWorker
from db.redis_storage import RedisStorageSingleton as RedisStorage
from gameengine.store.game_data import GameMetaData
from utils.load_config import load_config
from utils.logger import get_logger


class CommentaryManager:
    """
    Owns the lifecycle of per-game commentary workers and the shared store.

    Started/stopped alongside game schedulers so commentary follows the exact
    same lifecycle (including crash recovery).
    """

    def __init__(
        self,
        broker: MessageBroker,
        *,
        config: ConfigParser | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self._broker = broker
        self.config = config or load_config()
        self.logger = logger or get_logger(self.__class__.__name__)
        self.enabled = self.config.getboolean("commentary", "enabled", fallback=True)
        self._workers: dict[str, CommentaryWorker] = {}
        self._store: CommentaryStore | None = None

        if self.enabled:
            self._store = CommentaryStore(
                RedisStorage(self.config, self.logger),
                ttl_seconds=self.config.getint("commentary", "ttlSeconds", fallback=43200),
                max_lines=self.config.getint("commentary", "maxLinesPerGame", fallback=200),
                logger=self.logger,
            )

    @property
    def store(self) -> CommentaryStore | None:
        return self._store

    async def start_for_game(
        self,
        game_id: str,
        game_details: GameMetaData | None = None,
    ) -> None:
        if not self.enabled or game_id in self._workers:
            return
        worker = CommentaryWorker(
            game_id=game_id,
            broker=self._broker,
            config=self.config,
            game_details=game_details,
            store=self._store,
            logger=self.logger,
        )
        self._workers[game_id] = worker
        await worker.start()

    async def stop_for_game(self, game_id: str) -> None:
        worker = self._workers.pop(game_id, None)
        if worker is not None:
            await worker.stop()

    async def get_recent(self, game_id: str, limit: int = 10) -> list[dict[str, Any]]:
        if self._store is None:
            return []
        return await self._store.recent(game_id, limit=limit)

    async def shutdown(self) -> None:
        game_ids = list(self._workers.keys())
        if not game_ids:
            return
        await asyncio.gather(
            *(self.stop_for_game(game_id) for game_id in game_ids),
            return_exceptions=True,
        )
