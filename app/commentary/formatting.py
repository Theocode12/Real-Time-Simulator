from __future__ import annotations

from typing import TYPE_CHECKING, Any

from gameengine.store.game_data import TeamData

if TYPE_CHECKING:
    from app.commentary.events import CommentaryEvent

_POINT_NAMES = ("0", "15", "30", "40")

_REASON_PHRASES: dict[str, str] = {
    "ACE": "an ace",
    "DOUBLE_FAULT": "a double fault",
    "WINNER": "a clean winner",
    "FORCED_ERROR": "a forced error",
    "UNFORCED_ERROR": "an unforced error",
}


def team_name(team: TeamData | None) -> str:
    if team is None:
        return "the player"
    names = [player.name for player in team.players if player.name]
    if names:
        return " / ".join(names)
    return team.name or "the player"


def score_line(score: dict[str, Any]) -> str:
    points = score.get("game_points")
    if not isinstance(points, list) or len(points) != 2:
        return ""
    try:
        left, right = int(points[0]), int(points[1])
    except (TypeError, ValueError):
        return ""
    if left >= 3 and right >= 3:
        return "deuce" if left == right else "advantage"
    return f"{_POINT_NAMES[min(left, 3)]}-{_POINT_NAMES[min(right, 3)]}"


def reason_phrase(reason: Any) -> str:
    return _REASON_PHRASES.get(str(reason or "").upper(), "a well-constructed point")


def point_summary(event: CommentaryEvent) -> str:
    """One-line recap of a point, used as rolling context for providers."""
    point = event.point
    winner_side = point.get("winner")
    winner_team = team_name(event.teams.get("team_2") if winner_side == "TEAM_2" else event.teams.get("team_1"))
    player = point.get("player") or winner_team
    tags = ", ".join(event.tags) if event.tags else "routine"
    return f"{player} won ({reason_phrase(point.get('reason'))}; {tags})"
