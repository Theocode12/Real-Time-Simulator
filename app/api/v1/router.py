from fastapi import APIRouter

from app.api.v1 import live_games, media, support

router = APIRouter(prefix="/api/v1")

router.include_router(live_games.router, tags=["Live Games"])
router.include_router(media.router, tags=["Media"])
router.include_router(support.router, tags=["Support"])
