from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class CommentaryContext:
    """
    Rolling match memory handed to providers alongside the current point.

    Kept small and bounded on purpose: it is enough to give a provider a sense
    of what has just happened without unbounded memory growth.
    """

    recent_lines: tuple[str, ...] = ()
    recent_points: tuple[str, ...] = ()
    momentum_side: str | None = None
    streak: int = 0
