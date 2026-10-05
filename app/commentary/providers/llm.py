from __future__ import annotations

import json
import logging
from typing import Any

import httpx

from app.commentary.context import CommentaryContext
from app.commentary.events import CommentaryEvent
from app.commentary.formatting import reason_phrase, score_line, team_name
from app.commentary.schemas import CommentaryDraft
from utils.logger import get_logger

SYSTEM_PROMPT = (
    "You are a live tennis commentator. Given a single point of a simulated match, "
    "write exactly one short, vivid broadcast sentence (maximum 25 words). "
    "Use only the facts provided; never invent names, scores, or events. "
    "Do not repeat or paraphrase any phrasing from the recent commentary provided; "
    "vary your vocabulary between lines. "
    'Respond with strict JSON: {"text": string, "emotion": one of '
    '"neutral", "excited", "tense", "disappointed", "impressed"}.'
)

_ALLOWED_EMOTIONS = {"neutral", "excited", "tense", "disappointed", "impressed"}


def _situation(event: CommentaryEvent, context: CommentaryContext | None = None) -> str:
    teams = event.teams
    team_1 = team_name(teams.get("team_1"))
    team_2 = team_name(teams.get("team_2"))

    winner_side = event.point.get("winner")
    winner = team_2 if winner_side == "TEAM_2" else team_1
    player = event.point.get("player") or winner

    server_side = event.context.get("next_server")
    server = team_2 if server_side == "TEAM_2" else team_1

    tags = ", ".join(event.tags) if event.tags else "routine point"

    lines = [
        f"Match: {team_1} vs {team_2}",
        f"Current game score: {score_line(event.score) or 'n/a'}",
        f"Point won by {winner} ({player}) via {reason_phrase(event.point.get('reason'))}, "
        f"rally length {event.point.get('rally_length') or 'unknown'} shots",
        f"Situation: {tags}",
        f"Serving next: {server}",
    ]

    if context is not None:
        if context.recent_lines:
            lines.append("Recent commentary (do NOT repeat or paraphrase any of these):")
            lines.extend(f"- {line}" for line in context.recent_lines)
        if context.recent_points:
            lines.append("Recent points:")
            lines.extend(f"- {point}" for point in context.recent_points)

    return "\n".join(lines)


def _parse_content(content: Any) -> dict[str, Any]:
    if not isinstance(content, str):
        return {}

    text = content.strip()
    if text.startswith("```"):
        text = text[3:]
        if text.lower().startswith("json"):
            text = text[4:]
        text = text.strip()
        if text.endswith("```"):
            text = text[:-3].strip()

    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return {"text": content.strip()}

    return parsed if isinstance(parsed, dict) else {}


class LLMCommentaryProvider:
    """
    OpenAI-compatible chat-completions provider.

    Works with OpenAI, OpenRouter, vLLM, Ollama, etc. by pointing ``base_url``
    at the desired endpoint. Fails closed (returns ``None``) so callers can fall
    back to a deterministic provider.
    """

    source = "llm"

    def __init__(
        self,
        *,
        api_key: str = "",
        base_url: str = "https://api.openai.com/v1",
        model: str = "gpt-4.1-nano",
        timeout: float = 8.0,
        temperature: float = 0.7,
        max_tokens: int = 60,
        client: httpx.AsyncClient | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.model = model
        self.timeout = timeout
        self.temperature = temperature
        self.max_tokens = max_tokens
        self.logger = logger or get_logger(self.__class__.__name__)
        self._client = client
        self._owns_client = client is None

    def _client_or_create(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(base_url=self.base_url, timeout=self.timeout)
        return self._client

    async def aclose(self) -> None:
        if self._client is not None and self._owns_client:
            await self._client.aclose()
        self._client = None

    async def generate(
        self,
        event: CommentaryEvent,
        context: CommentaryContext | None = None,
    ) -> CommentaryDraft | None:
        payload: dict[str, Any] = {
            "model": self.model,
            "temperature": self.temperature,
            "max_tokens": self.max_tokens,
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": _situation(event, context)},
            ],
        }
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"

        try:
            client = self._client_or_create()
            response = await client.post("/chat/completions", json=payload, headers=headers)
            response.raise_for_status()
            content = response.json()["choices"][0]["message"]["content"]
            parsed = _parse_content(content)
        except Exception:
            self.logger.warning("LLM commentary call failed; falling back.", exc_info=True)
            return None

        text = str(parsed.get("text") or "").strip()
        if not text:
            return None

        emotion = str(parsed.get("emotion") or "neutral").strip().lower()
        if emotion not in _ALLOWED_EMOTIONS:
            emotion = "neutral"

        return CommentaryDraft(text=text, emotion=emotion)
