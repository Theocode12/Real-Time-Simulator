from fastapi import APIRouter

from app.api.v1 import game_controls, live_games, media, support

router = APIRouter(prefix="/api/v1")

router.include_router(live_games.router, tags=["Live Games"])
router.include_router(game_controls.router, tags=["Game Controls"])
router.include_router(media.router, tags=["Media"])
router.include_router(support.router, tags=["Support"])
