from typing import Annotated

from fastapi import APIRouter, Depends

from app.dependencies import get_support_service
from app.models.support import SupportCounts
from app.services.support_service import SupportService

router = APIRouter(prefix="/support", tags=["Support"])


@router.get("/game/{game_id}", response_model=SupportCounts)
async def get_game_support(
    game_id: str,
    support_service: Annotated[SupportService, Depends(get_support_service)],
) -> SupportCounts:
    """Return the current per-side support counts for a game."""
    return await support_service.get_counts(game_id)
