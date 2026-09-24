from __future__ import annotations

import asyncio
from configparser import ConfigParser
from typing import Any
from unittest.mock import MagicMock

import httpx
import pytest

from app.broker.memory_message_broker import InMemoryMessageBroker
from app.commentary.agent import CommentaryAgent
from app.commentary.context import CommentaryContext
from app.commentary.events import build_commentary_event
from app.commentary.manager import CommentaryManager
from app.commentary.providers.factory import create_commentary_provider
from app.commentary.providers.fallback import FallbackCommentaryProvider
from app.commentary.providers.llm import LLMCommentaryProvider, _situation
from app.commentary.providers.template import TemplateCommentaryProvider
from app.commentary.salience import (
    MOMENTUM_STREAK,
    importance_of,
    momentum_importance,
    should_commentate,
)
from app.commentary.worker import CommentaryWorker
from app.shared.enums.broker_channels import BrokerChannels
from gameengine.store.game_data import GameMetaData, PlayerData, TeamData

GAME_DETAILS = GameMetaData(
    game_id="g1",
    teams={
        "team_1": TeamData(name="Team One", players=[PlayerData(name="Alice")]),
        "team_2": TeamData(name="Team Two", players=[PlayerData(name="Bob")]),
    },
)


def make_message(
    point_index: int,
    *,
    reason: str = "WINNER",
    winner: str = "TEAM_1",
    is_match_point: bool = False,
    is_match_completed: bool = False,
) -> dict[str, Any]:
    return {
        "type": "game.score.update",
        "point_index": point_index,
        "data": {
            "score": {"set": [[3, 0], [1, 0]], "game_points": [3, 2]},
            "context": {
                "next_server": "TEAM_1",
                "is_match_completed": is_match_completed,
                "next_point_for": {"match": is_match_point, "set": False, "break": False, "game": False},
            },
            "point": {
                "winner": winner,
                "reason": reason,
                "player": "Alice" if winner == "TEAM_1" else "Bob",
                "rally_length": 5,
            },
        },
    }


def test_build_event_derives_ordered_tags() -> None:
    event = build_commentary_event("g1", make_message(7, reason="ACE", is_match_point=True), GAME_DETAILS)
    assert event.point_index == 7
    assert event.primary_tag == "match_point"
    assert "ace" in event.tags
    assert importance_of(event) == pytest.approx(0.95)


def test_salience_gate_skips_routine_points() -> None:
    routine_odd = build_commentary_event("g1", make_message(1), GAME_DETAILS)
    routine_even = build_commentary_event("g1", make_message(2), GAME_DETAILS)
    big_point = build_commentary_event("g1", make_message(3, is_match_point=True), GAME_DETAILS)

    assert not should_commentate(routine_odd, threshold=0.3, routine_interval=2)
    assert should_commentate(routine_even, threshold=0.3, routine_interval=2)
    assert should_commentate(big_point, threshold=0.3, routine_interval=2)


def test_momentum_curve_is_gentle() -> None:
    assert momentum_importance(MOMENTUM_STREAK - 1) == 0.0
    assert momentum_importance(MOMENTUM_STREAK) == pytest.approx(0.38)
    assert momentum_importance(MOMENTUM_STREAK + 1) == pytest.approx(0.46)
    assert momentum_importance(50) == pytest.approx(0.70)


def test_agent_applies_momentum_streak(valid_config: ConfigParser) -> None:
    agent = CommentaryAgent(
        game_id="g1",
        provider=TemplateCommentaryProvider(),
        broker=MagicMock(),
        config=valid_config,
        game_details=GAME_DETAILS,
    )

    event = None
    for index in range(MOMENTUM_STREAK):
        event = agent.build_event(make_message(index + 1, winner="TEAM_1"))

    assert event is not None
    assert event.momentum_streak == MOMENTUM_STREAK
    assert "momentum" in event.tags


def test_llm_situation_includes_history() -> None:
    event = build_commentary_event("g1", make_message(3, reason="ACE"), GAME_DETAILS)
    context = CommentaryContext(
        recent_lines=("Ace!",),
        recent_points=("Alice won (an ace; ace)",),
    )

    text = _situation(event, context)

    assert "Recent commentary" in text
    assert "Ace!" in text
    assert "Alice won (an ace; ace)" in text


@pytest.mark.asyncio
async def test_template_provider_uses_player_names() -> None:
    provider = TemplateCommentaryProvider()
    event = build_commentary_event("g1", make_message(1, reason="ACE"), GAME_DETAILS)

    # Draw every ace candidate so we deterministically cover the ones that
    # interpolate the player's name (some variants are generic).
    drafts = [await provider.generate(event) for _ in range(3)]

    assert all(draft is not None for draft in drafts)
    assert all(draft.emotion == "excited" for draft in drafts if draft)
    assert any("Alice" in draft.text for draft in drafts if draft)


@pytest.mark.asyncio
async def test_agent_publishes_commentary(valid_config: ConfigParser) -> None:
    broker = InMemoryMessageBroker(config=valid_config)
    agent = CommentaryAgent(
        game_id="g1",
        provider=TemplateCommentaryProvider(),
        broker=broker,
        config=valid_config,
        game_details=GAME_DETAILS,
    )

    iterator = await broker.subscribe("g1", BrokerChannels.COMMENTARY)
    line = await agent.handle_score(make_message(1, is_match_point=True))

    assert line is not None
    received = await asyncio.wait_for(iterator.__anext__(), timeout=1)
    assert received["type"] == "game.commentary"
    assert received["data"]["point_index"] == 1
    assert received["data"]["source"] == "template"


def _mock_client(handler: Any) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        transport=httpx.MockTransport(handler),
        base_url="https://api.openai.com/v1",
    )


@pytest.mark.asyncio
async def test_llm_provider_parses_json_response() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={"choices": [{"message": {"content": '{"text":"Ace!","emotion":"excited"}'}}]},
        )

    client = _mock_client(handler)
    provider = LLMCommentaryProvider(api_key="test", client=client)
    event = build_commentary_event("g1", make_message(1, reason="ACE"), GAME_DETAILS)

    draft = await provider.generate(event)

    assert draft is not None
    assert draft.text == "Ace!"
    assert draft.emotion == "excited"
    await client.aclose()


@pytest.mark.asyncio
async def test_llm_provider_returns_none_on_error() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, text="boom")

    client = _mock_client(handler)
    provider = LLMCommentaryProvider(api_key="test", client=client)
    event = build_commentary_event("g1", make_message(1, reason="ACE"), GAME_DETAILS)

    assert await provider.generate(event) is None
    await client.aclose()


class _DecliningProvider:
    source = "declining"

    async def generate(self, event: Any, context: Any = None) -> None:
        return None


@pytest.mark.asyncio
async def test_fallback_provider_uses_template() -> None:
    provider = FallbackCommentaryProvider(_DecliningProvider(), TemplateCommentaryProvider())
    event = build_commentary_event("g1", make_message(1, reason="ACE"), GAME_DETAILS)

    draft = await provider.generate(event)

    assert draft is not None
    assert draft.text


def test_factory_llm_without_key_falls_back_to_template(
    valid_config: ConfigParser,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    valid_config.add_section("commentary")
    valid_config.set("commentary", "provider", "llm")
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)

    provider = create_commentary_provider(valid_config)

    assert isinstance(provider, TemplateCommentaryProvider)


def test_factory_llm_with_key_wraps_fallback(
    valid_config: ConfigParser,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    valid_config.add_section("commentary")
    valid_config.set("commentary", "provider", "llm")
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")

    provider = create_commentary_provider(valid_config)

    assert isinstance(provider, FallbackCommentaryProvider)


@pytest.mark.asyncio
async def test_worker_generates_from_score_stream(valid_config: ConfigParser) -> None:
    broker = InMemoryMessageBroker(config=valid_config)
    worker = CommentaryWorker(
        game_id="g1",
        broker=broker,
        config=valid_config,
        game_details=GAME_DETAILS,
    )
    iterator = await broker.subscribe("g1", BrokerChannels.COMMENTARY)

    await worker.start()
    try:
        await broker.publish("g1", BrokerChannels.SCORES_UPDATE, make_message(1, is_match_point=True))
        received = await asyncio.wait_for(iterator.__anext__(), timeout=2)
        assert received["type"] == "game.commentary"
    finally:
        await worker.stop()


@pytest.mark.asyncio
async def test_manager_forwards_game_details_to_worker(
    valid_config: ConfigParser,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}

    class CapturingWorker:
        def __init__(
            self,
            *,
            game_id: str,
            broker: Any,
            config: Any,
            game_details: dict[str, Any] | None = None,
            store: Any = None,
            logger: Any = None,
        ) -> None:
            captured["game_id"] = game_id
            captured["game_details"] = game_details

        async def start(self) -> None:
            captured["started"] = True

    monkeypatch.setattr("app.commentary.manager.CommentaryWorker", CapturingWorker)

    manager = CommentaryManager(MagicMock(), config=valid_config)
    await manager.start_for_game("g1", GAME_DETAILS)

    assert captured["game_id"] == "g1"
    assert captured["game_details"] == GAME_DETAILS
    assert captured["started"] is True
