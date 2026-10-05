from __future__ import annotations

import pytest

from app.infra.keys import live_game, support_game, support_votes
from app.infra.support_store import SupportStore
from app.shared.enums.support import SupportSide


@pytest.mark.redis
@pytest.mark.asyncio
async def test_cast_vote_and_switch(redis_client) -> None:
    store = SupportStore(redis_client, ttl_seconds=60, live_registry_prefix="live:game")
    game_id = "support-test-game"

    await redis_client.set(live_game(game_id, "live:game"), "1")
    assert await store.is_game_live(game_id) is True

    counts = await store.cast_vote(game_id, "v1", SupportSide.TEAM_1)
    assert counts == {SupportSide.TEAM_1.value: 1, SupportSide.TEAM_2.value: 0}

    counts = await store.cast_vote(game_id, "v2", SupportSide.TEAM_1)
    assert counts == {SupportSide.TEAM_1.value: 2, SupportSide.TEAM_2.value: 0}

    # Moving v1's vote should decrement team_1 and increment team_2.
    counts = await store.cast_vote(game_id, "v1", SupportSide.TEAM_2)
    assert counts == {SupportSide.TEAM_1.value: 1, SupportSide.TEAM_2.value: 1}

    # Re-casting the same side is a no-op.
    counts = await store.cast_vote(game_id, "v1", SupportSide.TEAM_2)
    assert counts == {SupportSide.TEAM_1.value: 1, SupportSide.TEAM_2.value: 1}

    stored = await store.get_counts(game_id)
    assert stored == {SupportSide.TEAM_1.value: 1, SupportSide.TEAM_2.value: 1}

    # Both keys should carry a TTL.
    assert await redis_client.ttl(support_game(game_id)) > 0
    assert await redis_client.ttl(support_votes(game_id)) > 0
