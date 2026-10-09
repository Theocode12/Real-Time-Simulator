from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from app.api.v1.game_controls import router
from app.core.ws_auth import AuthService


@pytest.fixture
def token_app() -> FastAPI:
    app = FastAPI()
    app.include_router(router, prefix="/api/v1")
    app.state.sio_context = SimpleNamespace(
        scheduler_manager=MagicMock(),
        context=SimpleNamespace(auth=AuthService(secret="token-secret", token_ttl_seconds=120)),
    )
    return app


@pytest.mark.asyncio
async def test_control_token_endpoint_requires_api_secret(token_app: FastAPI, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("API_SECRET", "admin-secret")
    response = await _post_token(token_app, "game-a")
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_control_token_endpoint_issues_game_scoped_token(
    token_app: FastAPI,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("API_SECRET", "admin-secret")
    token_app.state.sio_context.scheduler_manager.has_scheduler.return_value = True

    response = await _post_token(token_app, "game-a", headers={"X-Secret": "admin-secret"})

    assert response.status_code == 200
    body = response.json()
    assert body["expires_in"] == 120
    auth = token_app.state.sio_context.context.auth
    assert auth.validate(body["token"])
    assert auth.validate_for_game(body["token"], "game-a")
    assert not auth.validate_for_game(body["token"], "game-b")


@pytest.mark.asyncio
async def test_control_token_endpoint_requires_active_game(
    token_app: FastAPI,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("API_SECRET", "admin-secret")
    token_app.state.sio_context.scheduler_manager.has_scheduler.return_value = False
    response = await _post_token(token_app, "game-a", headers={"X-Secret": "admin-secret"})
    assert response.status_code == 404


@pytest.mark.asyncio
async def test_control_token_endpoint_is_unavailable_without_signing_secret(
    token_app: FastAPI,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("API_SECRET", "admin-secret")
    monkeypatch.delenv("GAME_CONTROL_TOKEN_SECRET", raising=False)
    token_app.state.sio_context.scheduler_manager.has_scheduler.return_value = True
    token_app.state.sio_context.context.auth = AuthService()

    response = await _post_token(token_app, "game-a", headers={"X-Secret": "admin-secret"})

    assert response.status_code == 503


async def _post_token(app: FastAPI, game_id: str, headers: dict[str, str] | None = None):
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        return await client.post(f"/api/v1/games/{game_id}/control-token", headers=headers)
