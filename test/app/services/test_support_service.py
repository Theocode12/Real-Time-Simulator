from __future__ import annotations

import pytest

from app.exceptions.support_error import SupportGameNotLiveError
from app.services.support_service import SupportService
from app.shared.enums.support import SupportSide


class FakeSupportStore:
    """In-memory stand-in for SupportStore used to test service rules."""

    def __init__(self) -> None:
        self.live: dict[str, bool] = {}
        self.votes: dict[str, dict[str, str]] = {}
        self.counts: dict[str, dict[str, int]] = {}

    async def is_game_live(self, game_id: str) -> bool:
        return self.live.get(game_id, False)

    async def get_counts(self, game_id: str) -> dict[str, int]:
        return dict(self._counts(game_id))

    async def cast_vote(self, game_id: str, voter_id: str, side: SupportSide) -> dict[str, int]:
        votes = self.votes.setdefault(game_id, {})
        counts = self._counts(game_id)

        previous = votes.get(voter_id)
        if previous == side.value:
            return dict(counts)

        if previous:
            counts[previous] -= 1

        counts[side.value] += 1
        votes[voter_id] = side.value
        return dict(counts)

    def _counts(self, game_id: str) -> dict[str, int]:
        return self.counts.setdefault(
            game_id,
            {SupportSide.TEAM_1.value: 0, SupportSide.TEAM_2.value: 0},
        )


@pytest.fixture
def store() -> FakeSupportStore:
    return FakeSupportStore()


@pytest.fixture
def service(store: FakeSupportStore) -> SupportService:
    return SupportService(store)


@pytest.mark.asyncio
async def test_get_counts_defaults_to_zero(service: SupportService) -> None:
    counts = await service.get_counts("g1")
    assert counts.team_1 == 0
    assert counts.team_2 == 0
    assert counts.total == 0


@pytest.mark.asyncio
async def test_cast_vote_requires_live_game(service: SupportService) -> None:
    with pytest.raises(SupportGameNotLiveError):
        await service.cast_vote("g1", "v1", SupportSide.TEAM_1)


@pytest.mark.asyncio
async def test_cast_vote_increments_side(service: SupportService, store: FakeSupportStore) -> None:
    store.live["g1"] = True

    counts = await service.cast_vote("g1", "v1", SupportSide.TEAM_1)
    assert counts.team_1 == 1
    assert counts.team_2 == 0
    assert counts.total == 1

    counts = await service.cast_vote("g1", "v2", SupportSide.TEAM_2)
    assert counts.team_1 == 1
    assert counts.team_2 == 1
    assert counts.total == 2


@pytest.mark.asyncio
async def test_switch_side_moves_vote(service: SupportService, store: FakeSupportStore) -> None:
    store.live["g1"] = True

    await service.cast_vote("g1", "v1", SupportSide.TEAM_1)
    counts = await service.cast_vote("g1", "v1", SupportSide.TEAM_2)

    assert counts.team_1 == 0
    assert counts.team_2 == 1
    assert counts.total == 1


@pytest.mark.asyncio
async def test_recording_same_side_is_idempotent(service: SupportService, store: FakeSupportStore) -> None:
    store.live["g1"] = True

    await service.cast_vote("g1", "v1", SupportSide.TEAM_1)
    counts = await service.cast_vote("g1", "v1", SupportSide.TEAM_1)

    assert counts.team_1 == 1
    assert counts.total == 1
