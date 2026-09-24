from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel

from app.core.context import AppContext
from app.handlers.base import BaseHandler
from app.shared.enums.broker_channels import BrokerChannels
from app.shared.enums.game_event import GameEvent


async def _process_broker_message(
    message: dict[str, Any],
) -> tuple[GameEvent, dict[str, Any]] | None:
    """Processes a raw message from the broker into a client-facing event."""
    msg_type = message.get("type")
    if not msg_type:
        return None

    if not isinstance(msg_type, GameEvent):
        try:
            msg_type = GameEvent(msg_type)  # normalize string -> Event
        except ValueError:
            return None

    event: GameEvent = msg_type
    return event, message


class JoinGameHandler(BaseHandler):
    """
    Handles a client's request to join a game.

    Verifies the game is active, starts a broker listener if needed,
    adds the client to the appropriate Socket.IO room, and sends back game metadata.
    """

    context: AppContext

    async def handle(self, sid: str, data: dict[str, Any]) -> None:
        """
        Process a 'join game' request from a client and respond with game metadata.

        Args:
            sid (str): Socket.IO session ID of the client.
            data (dict): Incoming request payload containing at least 'game_id'.
        """
        context = self.context
        logger = context.logger
        namespace = data.get("namespace", "/game")
        game_id = data.get("game_id")

        if not game_id:
            logger.warning(f"JoinGameHandler: Missing game_id in client data from {sid}")
            await context.sio.emit(
                GameEvent.ERROR,
                {"error": "Missing required 'game_id' field."},
                to=sid,
                namespace=namespace,
            )
            return

        # scheduler = context.scheduler_manager.get_scheduler(game_id)
        # if not scheduler:
        if not context.scheduler_manager.has_scheduler(game_id):
            logger.warning(f"JoinGameHandler: Game '{game_id}' not found or inactive.")
            await context.sio.emit(
                GameEvent.ERROR.value,
                {"error": f"Game '{game_id}' is not currently active or does not exist."},
                to=sid,
                namespace=namespace,
            )
            return

        channels_str = context.config.get("broker", "relay_channels", fallback="SCORES_UPDATE,CONTROLS")
        try:
            channels_to_listen = [BrokerChannels(c.strip().lower()) for c in channels_str.split(",") if c.strip()]
        except ValueError as e:
            logger.error(f"Invalid broker channel in config: {e}. Using default channels.")
            channels_to_listen = [
                BrokerChannels.SCORES_UPDATE,
                BrokerChannels.CONTROLS,
            ]

        # The BrokerRelay ensures a listener is started
        # only once per game/channel set.
        await context.broker_relay.start_listener(game_id, channels_to_listen, namespace, _process_broker_message)

        try:
            await context.sio.enter_room(sid, game_id, namespace=namespace)
            logger.info(f"JoinGameHandler: Client {sid} entered Socket.IO room {game_id}")

            response_data = await context.scheduler_manager.get_game_data(game_id)
            if not response_data:
                raise RuntimeError(f"Failed to retrieve metadata for game '{game_id}'")

            await context.sio.emit(
                GameEvent.GAME_JOIN,
                {**response_data, "message": f"Successfully joined game {game_id}"},
                to=sid,
                namespace=namespace,
            )

            await self._replay_commentary(context, sid, game_id, namespace)
        except Exception as e:
            logger.error(
                f"JoinGameHandler: Failed to add client {sid} to room {game_id}: {e}",
                exc_info=True,
            )
            await context.sio.emit(
                GameEvent.ERROR,
                {"error": f"Failed to enter game room '{game_id}'."},
                to=sid,
                namespace=namespace,
            )

    async def _replay_commentary(
        self,
        context: AppContext,
        sid: str,
        game_id: str,
        namespace: str,
    ) -> None:
        """Send the most recent commentary lines to a client that just joined."""
        commentary_manager = getattr(context, "commentary_manager", None)
        if commentary_manager is None:
            return

        try:
            recent = await commentary_manager.get_recent(game_id, limit=10)
        except Exception:
            context.logger.debug("Commentary replay failed for game %s", game_id, exc_info=True)
            return

        for line in recent:
            await context.sio.emit(
                GameEvent.GAME_COMMENTARY,
                {"type": GameEvent.GAME_COMMENTARY.value, "data": line},
                to=sid,
                namespace=namespace,
            )


class JoinGameSchema(BaseModel):
    game_id: str
    type: Literal[GameEvent.GAME_JOIN]
