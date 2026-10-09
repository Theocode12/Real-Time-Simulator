from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, Request

from app.dependencies import require_secret

router = APIRouter()


@router.post("/games/{game_id}/control-token")
async def issue_game_control_token(
    game_id: str,
    request: Request,
    _secret: Annotated[None, Depends(require_secret)],
) -> dict[str, Any]:
    """Mint an owner token for an active game for trusted control-plane callers."""
    sio_context = getattr(request.app.state, "sio_context", None)
    if sio_context is None:
        raise HTTPException(status_code=503, detail="Game control service unavailable")

    scheduler_manager = sio_context.scheduler_manager
    if not scheduler_manager.has_scheduler(game_id):
        raise HTTPException(status_code=404, detail="Game not found or not running")

    auth = sio_context.context.auth
    if not auth.configured:
        raise HTTPException(status_code=503, detail="Game control token service unavailable")

    return {
        "token": auth.issue_token(game_id),
        "expires_in": auth.token_ttl_seconds,
    }
