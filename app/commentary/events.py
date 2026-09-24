from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from gameengine.store.game_data import GameMetaData, TeamData

# Ordered by descending editorial importance; ``primary_tag`` is the first match.
TAG_PRIORITY: tuple[str, ...] = (
    "match_won",
    "match_point",
    "set_won",
    "set_point",
    "break_point",
    "game_won",
    "tiebreak",
    "deuce",
    "ace",
    "double_fault",
    "long_rally",
)

LONG_RALLY_SHOTS = 10


@dataclass(frozen=True, slots=True)
class CommentaryEvent:
    """Normalized, provider-agnostic view of a single simulated point."""

    game_id: str
    point_index: int
    point: dict[str, Any]
    context: dict[str, Any]
    score: dict[str, Any]
    teams: dict[str, TeamData] = field(default_factory=dict)
    tags: tuple[str, ...] = ()
    momentum_streak: int = 0

    @property
    def primary_tag(self) -> str:
        return self.tags[0] if self.tags else "default"


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _derive_tags(point: dict[str, Any], context: dict[str, Any]) -> tuple[str, ...]:
    found: set[str] = set()
    reason = str(point.get("reason") or "").upper()
    next_point_for = _as_dict(context.get("next_point_for"))

    if context.get("is_match_completed"):
        found.add("match_won")
    if next_point_for.get("match"):
        found.add("match_point")
    if context.get("is_set_completed"):
        found.add("set_won")
    if next_point_for.get("set"):
        found.add("set_point")
    if next_point_for.get("break"):
        found.add("break_point")
    if context.get("is_game_completed"):
        found.add("game_won")
    if context.get("is_tiebreak"):
        found.add("tiebreak")
    if context.get("is_deuce"):
        found.add("deuce")
    if reason == "ACE":
        found.add("ace")
    if reason == "DOUBLE_FAULT":
        found.add("double_fault")

    try:
        if int(point.get("rally_length") or 0) >= LONG_RALLY_SHOTS:
            found.add("long_rally")
    except (TypeError, ValueError):
        pass

    return tuple(tag for tag in TAG_PRIORITY if tag in found)


def build_commentary_event(
    game_id: str,
    message: dict[str, Any],
    game_details: GameMetaData | None,
) -> CommentaryEvent:
    """Translate a raw ``game.score.update`` broker message into an event."""
    data = _as_dict(message.get("data"))
    point = _as_dict(data.get("point"))
    context = _as_dict(data.get("context"))
    score = _as_dict(data.get("score"))

    try:
        point_index = int(message.get("point_index") or 0)
    except (TypeError, ValueError):
        point_index = 0

    return CommentaryEvent(
        game_id=game_id,
        point_index=point_index,
        point=point,
        context=context,
        score=score,
        teams=game_details.teams if game_details is not None else {},
        tags=_derive_tags(point, context),
    )
