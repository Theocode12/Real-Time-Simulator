from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any

from pydantic import ValidationError

from app.exceptions.support_error import SupportError
from app.models.support import SupportCastRequest, SupportCounts, SupportJoinRequest
from app.shared.enums.game_event import GameEvent

from .base_namespace import BaseNamespace

if TYPE_CHECKING:
    from app.core.context import AppContext

_CAST_MIN_INTERVAL_SECONDS = 0.2


class SupportNamespace(BaseNamespace):
    """
    Live "support a player" namespace.

    Viewers join a match room and cast a vote for one side. Per-side counts
    are broadcast to the room. The final good/bad verdict is derived by the
    frontend from the score stream, so no result is emitted here.
    """

    def __init__(self, namespace: str, context: AppContext) -> None:
        super().__init__(namespace, context)

        # sid -> {"game_id": str, "voter_id": str}
        self._sessions: dict[str, dict[str, str]] = {}
        # sid -> monotonic timestamp of the last cast (best-effort throttle)
        self._last_cast: dict[str, float] = {}

    # =========================
    # Connection Lifecycle
    # =========================

    async def on_connect(self, sid: str, environ: dict[str, Any]) -> None:
        self.logger.debug(f"[support] Client connected: SID={sid}")

    async def on_disconnect(self, sid: str) -> None:
        session = self._sessions.pop(sid, None)
        self._last_cast.pop(sid, None)

        if not session:
            return

        game_id = session["game_id"]
        self.logger.debug(f"[support] Client {sid} disconnected from game {game_id}")
        await self.leave_room(sid, game_id)

    # =========================
    # Client Events
    # =========================

    async def on_join(self, sid: str, data: dict[str, Any]) -> None:
        """
        Subscribe to a match's support board.

        Expected payload:
        {
            "game_id": "<str>",
            "voter_id": "<optional str>"
        }
        """
        request = self._parse(data, SupportJoinRequest)
        if request is None:
            await self.emit_error(sid, "Invalid support join payload.")
            return

        voter_id = self._resolve_voter_id(sid, request.voter_id)
        self._sessions[sid] = {"game_id": request.game_id, "voter_id": voter_id}

        await self.enter_room(sid, request.game_id)

        counts = await self.context.support.get_counts(request.game_id)
        await self._emit_counts(request.game_id, counts, to=sid)

    async def on_cast(self, sid: str, data: dict[str, Any]) -> None:
        """
        Cast (or move) a support vote for a match.

        Expected payload:
        {
            "game_id": "<str>",
            "side": "team_1" | "team_2",
            "voter_id": "<optional str>"
        }
        """
        request = self._parse(data, SupportCastRequest)
        if request is None:
            await self.emit_error(sid, "Invalid support cast payload.")
            return

        now = time.monotonic()
        if now - self._last_cast.get(sid, 0.0) < _CAST_MIN_INTERVAL_SECONDS:
            return
        self._last_cast[sid] = now

        voter_id = self._resolve_voter_id(sid, request.voter_id)
        self._sessions[sid] = {"game_id": request.game_id, "voter_id": voter_id}

        try:
            counts = await self.context.support.cast_vote(request.game_id, voter_id, request.side)
        except SupportError as e:
            await self.emit_error(sid, str(e))
            return
        except Exception:
            self.logger.exception(f"[support] Failed to cast vote for game {request.game_id}")
            await self.emit_error(sid, "Failed to record support.")
            return

        await self.enter_room(sid, request.game_id)
        await self._emit_counts(request.game_id, counts)

    # =========================
    # Internal Helpers
    # =========================

    @staticmethod
    def _parse(data: Any, schema: type) -> Any:
        if not isinstance(data, dict):
            return None
        try:
            return schema(**data)
        except ValidationError:
            return None

    async def _emit_counts(self, game_id: str, counts: SupportCounts, to: str | None = None) -> None:
        payload = {"game_id": game_id, **counts.model_dump()}
        if to:
            await self.emit(GameEvent.SUPPORT_UPDATE, payload, to=to)
        else:
            await self.emit(GameEvent.SUPPORT_UPDATE, payload, room=game_id)

    @staticmethod
    def _resolve_voter_id(sid: str, voter_id: str | None) -> str:
        if voter_id and voter_id.strip():
            return voter_id.strip()[:64]
        return sid
