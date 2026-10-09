from __future__ import annotations

from typing import Any

from app.shared.enums.game_event import GameEvent

from .base import BaseHandler


class AuthenticatedHandler(BaseHandler):
    async def handle(self, sid: str, data: dict[str, Any]) -> None:
        token: str = data.get("token", "")
        game_id = data.get("game_id", "")
        if not self.context.auth.validate(token) or not self.context.auth.validate_for_game(token, game_id):
            await self.context.sio.emit(
                GameEvent.ERROR,
                {"error": "Unauthorized"},
                to=sid,
                namespace=data.get("namespace", ""),
            )
            return
        await self.handle_authenticated(sid, data)

    async def handle_authenticated(self, sid: str, data: dict[str, Any]) -> None:
        raise NotImplementedError
