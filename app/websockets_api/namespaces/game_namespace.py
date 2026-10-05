from __future__ import annotations

from typing import TYPE_CHECKING, Any

from app.exceptions.message_error import MessageError

from .base_namespace import BaseNamespace

if TYPE_CHECKING:
    from app.core.context import AppContext


class GameNamespace(BaseNamespace):
    """
    Handles all client interactions within the '/game' namespace.

    Manages connections, disconnections, message routing, validation,
    and cleanup for game-related interactions.
    """

    context: AppContext

    async def on_connect(self, sid: str, environ: dict[str, Any]) -> None:
        """
        Handles a new client connection to the '/game' namespace.

        Args:
            sid: The session ID of the client.
            environ: The environment dictionary provided by the connection.
        """
        # TODO: Implement authentication/validation logic using environ or an
        #        initial auth message if needed
        self.logger.debug(f"[game] Client connected: SID={sid}")

    async def on_disconnect(self, sid: str) -> None:
        """
        Handles a client disconnection from the '/game' namespace.

        Args:
            sid: The session ID of the disconnected client.
        """
        try:
            # Retrieve the rooms the client is part of
            client_rooms = self.context.sio.rooms(sid, namespace=self.namespace)
            self.logger.debug(f"Client {sid} was in rooms: {client_rooms}")

            for room in client_rooms:
                if room == sid:
                    continue  # Skip the client's default room

                self.logger.info(f"Removing client {sid} from room {room} in namespace {self.namespace}")
                await self.leave_room(sid, room)

        except Exception as e:
            self.logger.error(f"Error during disconnect cleanup for SID {sid}: {e}", exc_info=True)

    async def on_game(self, sid: str, data: Any) -> None:
        self.logger.debug(f"Received 'message' event on {self.namespace} from SID {sid}: {data}")
        try:
            if not isinstance(data, dict):
                raise MessageError("Data must be of type dict.")
            await self.dispatcher.dispatch(sid, data, self.namespace)
        except MessageError as e:
            self.logger.error(f"MessageError in {self.namespace} for SID {sid}: {e}")
            await self.emit_error(sid, str(e))
        except Exception as e:
            self.logger.exception(f"Error processing message in {self.namespace} for SID {sid}: {e}")
            await self.emit_error(sid, "Internal server error")
