from __future__ import annotations

from app.commentary.events import CommentaryEvent

BASE_IMPORTANCE = 0.2

MOMENTUM_STREAK = 3
MOMENTUM_BASE = 0.30
MOMENTUM_STEP = 0.08
MOMENTUM_CAP = 0.70

_TAG_IMPORTANCE: dict[str, float] = {
    "match_won": 1.0,
    "match_point": 0.95,
    "set_won": 0.9,
    "set_point": 0.85,
    "break_point": 0.8,
    "deuce": 0.7,
    "tiebreak": 0.65,
    "game_won": 0.6,
    "ace": 0.55,
    "double_fault": 0.5,
    "long_rally": 0.45,
}


def momentum_importance(streak: int) -> float:
    """Gentle curve: rises from the streak threshold and caps out."""
    if streak < MOMENTUM_STREAK:
        return 0.0
    return min(MOMENTUM_BASE + MOMENTUM_STEP * (streak - (MOMENTUM_STREAK - 1)), MOMENTUM_CAP)


def importance_of(event: CommentaryEvent) -> float:
    """Return the editorial importance of an event in the range ``0.0 - 1.0``."""
    base = BASE_IMPORTANCE
    for tag in event.tags:
        if tag == "momentum":
            continue
        base = max(base, _TAG_IMPORTANCE.get(tag, BASE_IMPORTANCE))
    return max(base, momentum_importance(event.momentum_streak))


def should_commentate(
    event: CommentaryEvent,
    *,
    threshold: float,
    routine_interval: int,
) -> bool:
    """
    Decide whether a point deserves commentary.

    High-salience points always pass; routine points pass periodically so the
    feed stays alive without calling the provider on every single point.
    """
    if importance_of(event) >= threshold:
        return True
    if routine_interval > 0 and event.point_index > 0 and event.point_index % routine_interval == 0:
        return True
    return False
