from __future__ import annotations

import random
from collections import deque
from typing import Any

from app.commentary.context import CommentaryContext
from app.commentary.events import CommentaryEvent
from app.commentary.formatting import reason_phrase, score_line, team_name
from app.commentary.schemas import CommentaryDraft

_EMOTION_BY_TAG: dict[str, str] = {
    "match_won": "excited",
    "set_won": "excited",
    "match_point": "tense",
    "set_point": "tense",
    "break_point": "tense",
    "deuce": "tense",
    "tiebreak": "tense",
    "ace": "excited",
    "double_fault": "disappointed",
    "long_rally": "impressed",
    "game_won": "neutral",
}


def _build_context(event: CommentaryEvent) -> dict[str, Any]:
    team_1 = event.teams.get("team_1")
    team_2 = event.teams.get("team_2")

    if event.point.get("winner") == "TEAM_2":
        winner_team, loser_team = team_name(team_2), team_name(team_1)
    else:
        winner_team, loser_team = team_name(team_1), team_name(team_2)

    if event.context.get("next_server") == "TEAM_2":
        server_team, receiver_team = team_name(team_2), team_name(team_1)
    else:
        server_team, receiver_team = team_name(team_1), team_name(team_2)

    return {
        "winner_team": winner_team,
        "loser_team": loser_team,
        "server_team": server_team,
        "receiver_team": receiver_team,
        "player": event.point.get("player") or winner_team,
        "score_line": score_line(event.score),
        "rally_length": event.point.get("rally_length") or 0,
        "reason_phrase": reason_phrase(event.point.get("reason")),
    }


def _candidates(tag: str, context: dict[str, Any]) -> list[str]:
    winner = context["winner_team"]
    loser = context["loser_team"]
    receiver = context["receiver_team"]
    player = context["player"]
    rally = context["rally_length"]
    reason_phrase = context["reason_phrase"]

    if tag == "match_won":
        return [
            f"{winner} win the match!",
            f"Game, set and match — {winner}!",
            f"It's all over. {winner} take it.",
        ]
    if tag == "set_won":
        return [
            f"{winner} take the set.",
            f"Set {winner}.",
            f"{winner} close out the set.",
        ]
    if tag == "match_point":
        return [
            f"Match point for {winner}!",
            f"{winner} have a match point.",
        ]
    if tag == "set_point":
        return [
            f"Set point for {winner}.",
            f"{winner} with a set point.",
        ]
    if tag == "break_point":
        return [
            f"Break point for {receiver}.",
            f"{receiver} have a look at a break.",
        ]
    if tag == "game_won":
        return [
            f"Game, {winner}.",
            f"{winner} take the game.",
        ]
    if tag == "tiebreak":
        return [
            "We're into a tiebreak.",
            "Tiebreak time — no room for error.",
        ]
    if tag == "deuce":
        return [
            "Deuce.",
            "Back to deuce.",
            "Deuce again — the pressure is immense.",
        ]
    if tag == "ace":
        return [
            f"Ace! {player} finds the line.",
            f"{player} fires an ace.",
            "Unreturnable — ace!",
        ]
    if tag == "double_fault":
        return [
            f"Double fault from {loser}.",
            "A double fault hands the point away.",
            f"{loser} double fault under pressure.",
        ]
    if tag == "long_rally":
        return [
            f"A {rally}-shot rally, and {player} comes out on top.",
            f"What a rally — {rally} shots!",
        ]
    return [
        f"{player} takes the point with {reason_phrase}.",
        f"Point {winner}, {reason_phrase}.",
        f"{player} comes through.",
    ]


class TemplateCommentaryProvider:
    """Deterministic, rules-based commentary provider with repetition control."""

    source = "template"

    def __init__(self, *, max_recent: int = 6) -> None:
        self._recent: deque[str] = deque(maxlen=max_recent)

    async def generate(
        self,
        event: CommentaryEvent,
        context: CommentaryContext | None = None,
    ) -> CommentaryDraft | None:
        tag = event.primary_tag
        candidates = _candidates(tag, _build_context(event))
        if not candidates:
            return None

        excluded = set(self._recent)
        if context is not None:
            excluded.update(context.recent_lines)

        fresh = [line for line in candidates if line not in excluded]
        text = random.choice(fresh or candidates)
        self._recent.append(text)

        return CommentaryDraft(text=text, emotion=_EMOTION_BY_TAG.get(tag, "neutral"))
