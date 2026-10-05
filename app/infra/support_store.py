from __future__ import annotations

import logging
from collections.abc import Awaitable
from typing import Any, cast

import redis.asyncio as redis

from app.infra.keys import live_game, support_game, support_votes
from app.shared.enums.support import SupportSide
from utils.logger import get_logger

_SIDE_SWITCH_LUA = """
local prev = redis.call('HGET', KEYS[2], ARGV[1])
if prev == ARGV[2] then
    redis.call('EXPIRE', KEYS[1], ARGV[5])
    redis.call('EXPIRE', KEYS[2], ARGV[5])
    return redis.call('HMGET', KEYS[1], ARGV[3], ARGV[4])
end
if prev then
    redis.call('HINCRBY', KEYS[1], prev, -1)
end
redis.call('HINCRBY', KEYS[1], ARGV[2], 1)
redis.call('HSET', KEYS[2], ARGV[1], ARGV[2])
redis.call('EXPIRE', KEYS[1], ARGV[5])
redis.call('EXPIRE', KEYS[2], ARGV[5])
return redis.call('HMGET', KEYS[1], ARGV[3], ARGV[4])
"""


class SupportStore:
    """
    Encapsulates all Redis communication for the support feature.

    Persists per-game support counts and each voter's chosen side, and
    performs the side-switch atomically via a Lua script so counts cannot
    drift under concurrent votes.
    """

    def __init__(
        self,
        redis_client: redis.Redis,
        *,
        ttl_seconds: int,
        live_registry_prefix: str,
        logger: logging.Logger | None = None,
    ) -> None:
        self.redis_client = redis_client
        self.ttl_seconds = ttl_seconds
        self.live_registry_prefix = live_registry_prefix
        self.logger = logger or get_logger(self.__class__.__name__)
        self._side_switch = redis_client.register_script(_SIDE_SWITCH_LUA)

    async def is_game_live(self, game_id: str) -> bool:
        """Return True when the live registry still holds the game."""
        return bool(await self.redis_client.exists(live_game(game_id, self.live_registry_prefix)))

    async def cast_vote(self, game_id: str, voter_id: str, side: SupportSide) -> dict[str, int]:
        """Record or move a vote and return the updated per-side counts."""
        raw = await self._side_switch(
            keys=[support_game(game_id), support_votes(game_id)],
            args=[
                voter_id,
                side.value,
                SupportSide.TEAM_1.value,
                SupportSide.TEAM_2.value,
                int(self.ttl_seconds),
            ],
        )
        return self._parse_counts(raw)

    async def get_counts(self, game_id: str) -> dict[str, int]:
        """Return the current per-side counts (zeros when no votes exist)."""
        pending = self.redis_client.hmget(
            support_game(game_id),
            [SupportSide.TEAM_1.value, SupportSide.TEAM_2.value],
        )
        raw = await cast(Awaitable[list[Any]], pending)
        return self._parse_counts(raw)

    @staticmethod
    def _parse_counts(raw: Any) -> dict[str, int]:
        if not raw:
            return {SupportSide.TEAM_1.value: 0, SupportSide.TEAM_2.value: 0}

        def _to_int(value: Any) -> int:
            try:
                return int(value)
            except (TypeError, ValueError):
                return 0

        values = list(raw)
        while len(values) < 2:
            values.append(0)

        return {
            SupportSide.TEAM_1.value: _to_int(values[0]),
            SupportSide.TEAM_2.value: _to_int(values[1]),
        }
