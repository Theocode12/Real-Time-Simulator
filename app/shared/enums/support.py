from __future__ import annotations

from enum import StrEnum, unique


@unique
class SupportSide(StrEnum):
    """The side a viewer can support within a match."""

    TEAM_1 = "team_1"
    TEAM_2 = "team_2"
