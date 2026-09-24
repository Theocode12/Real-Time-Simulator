from __future__ import annotations

from unittest.mock import AsyncMock

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from app.api.v1.support import router as support_router
from app.dependencies import get_support_service
from app.models.support import SupportCounts


@pytest.fixture
def fake_service() -> AsyncMock:
    service = AsyncMock()
    service.get_counts = AsyncMock(return_value=SupportCounts(team_1=3, team_2=1, total=4))
    return service


@pytest.fixture
def app(fake_service: AsyncMock) -> FastAPI:
    test_app = FastAPI()
    test_app.include_router(support_router, prefix="/api/v1")
    test_app.dependency_overrides[get_support_service] = lambda: fake_service
    return test_app


@pytest.fixture
async def client(app: FastAPI):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as c:
        yield c


@pytest.mark.asyncio
async def test_get_game_support(client: AsyncClient, fake_service: AsyncMock) -> None:
    response = await client.get("/api/v1/support/game/g1")

    assert response.status_code == 200
    assert response.json() == {"team_1": 3, "team_2": 1, "total": 4}
    fake_service.get_counts.assert_awaited_once_with("g1")
