from __future__ import annotations

import json
import logging
from typing import Any

from db.redis_storage import RedisStorageBase
from utils.logger import get_logger


class CommentaryStore:
    """
    Persists per-game commentary history in Redis.

    All operations are best-effort: a Redis failure must never interrupt the
    game loop or the commentary worker, so errors are logged and swallowed.
    """

    def __init__(
        self,
        storage: RedisStorageBase,
        *,
        ttl_seconds: int = 43200,
        max_lines: int = 200,
        logger: logging.Logger | None = None,
    ) -> None:
        self._storage = storage
        self._ttl = ttl_seconds
        self._max_lines = max_lines
        self.logger = logger or get_logger(self.__class__.__name__)

    @staticmethod
    def key(game_id: str) -> str:
        return f"{game_id}:commentary"

    async def _client(self) -> Any:
        if not self._storage.is_connected():
            await self._storage.connect()
        return self._storage.get_client()

    async def append(self, game_id: str, line: dict[str, Any]) -> None:
        try:
            client = await self._client()
            key = self.key(game_id)
            await client.rpush(key, json.dumps(line))
            await client.ltrim(key, -self._max_lines, -1)
            await client.expire(key, self._ttl)
        except Exception:
            self.logger.debug("Failed to persist commentary line for game %s", game_id, exc_info=True)

    async def recent(self, game_id: str, limit: int = 10) -> list[dict[str, Any]]:
        try:
            client = await self._client()
            raw = await client.lrange(self.key(game_id), -limit, -1)
        except Exception:
            self.logger.debug("Failed to read commentary history for game %s", game_id, exc_info=True)
            return []

        lines: list[dict[str, Any]] = []
        for item in raw:
            try:
                parsed = json.loads(item)
            except (json.JSONDecodeError, TypeError):
                continue
            if isinstance(parsed, dict):
                lines.append(parsed)
        return lines
