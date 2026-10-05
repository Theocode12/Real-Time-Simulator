from __future__ import annotations

from pydantic import BaseModel, Field

from app.shared.enums.support import SupportSide


class SupportJoinRequest(BaseModel):
    game_id: str = Field(..., min_length=1)
    voter_id: str | None = None


class SupportCastRequest(BaseModel):
    game_id: str = Field(..., min_length=1)
    side: SupportSide
    voter_id: str | None = None


class SupportCounts(BaseModel):
    team_1: int = 0
    team_2: int = 0
    total: int = 0

    @classmethod
    def from_raw(cls, counts: dict[str, int]) -> SupportCounts:
        team_1 = int(counts.get(SupportSide.TEAM_1.value, 0))
        team_2 = int(counts.get(SupportSide.TEAM_2.value, 0))
        return cls(team_1=team_1, team_2=team_2, total=team_1 + team_2)
