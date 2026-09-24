from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any

from socketio import AsyncServer  # type: ignore

from app.broker.message_broker import MessageBroker, OverflowPolicy
from app.shared.enums.broker_channels import BrokerChannels
from app.shared.enums.game_event import GameEvent

if TYPE_CHECKING:
    pass

MessageProcessor = Callable[[dict[str, Any]], Awaitable[tuple[str, dict[str, Any]] | None]]


class BrokerRelay:
    def __init__(self, sio: AsyncServer, broker: MessageBroker, logger: logging.Logger):
        self._sio = sio
        self._broker = broker
        self._logger = logger
        self._tasks: dict[str, asyncio.Task[None]] = {}
        self._lock = asyncio.Lock()

    def _create_subscription_key(self, game_id: str, channels: list[BrokerChannels]) -> str:
        return f"{game_id}:{'+'.join(sorted(c.value for c in channels))}"

    async def start_listener(
        self,
        game_id: str,
        channels: list[BrokerChannels],
        namespace: str,
        processor: MessageProcessor,
    ) -> None:
        key = self._create_subscription_key(game_id, channels)
        async with self._lock:
            if key in self._tasks:
                self._logger.debug(f"Reusing existing broker relay for {key}.")
                return

            task = asyncio.create_task(
                self._listener(game_id, channels, namespace, processor),
                name=f"broker_relay_{key}",
            )
            self._tasks[key] = task

            def _done_callback(t: asyncio.Task[None]) -> None:
                self._tasks.pop(key, None)
                self._logger.debug(f"Broker relay task removed: {key}")

            task.add_done_callback(_done_callback)
            self._logger.info(f"Broker relay started for {key}.")

    async def _listener(
        self,
        game_id: str,
        channels: list[BrokerChannels],
        namespace: str,
        processor: MessageProcessor,
    ) -> None:
        try:
            # Prefer the freshest stream to clients; score payloads are cumulative
            # snapshots, so dropping an intermediate update self-heals on the next.
            iterator = await self._broker.subscribe(game_id, channels, policy=OverflowPolicy.DROP_OLD)
            async for message in iterator:
                if not isinstance(message, dict):
                    continue

                result = await processor(message)
                if not result:
                    continue

                event_name, payload = result
                await self._sio.emit(event_name, payload, room=game_id, namespace=namespace)

            await self._sio.emit(
                GameEvent.GAME_END.value,
                {"game_id": game_id},
                room=game_id,
                namespace=namespace,
            )
        except asyncio.CancelledError:
            self._logger.debug(f"Broker relay cancelled for {game_id}.")
            raise
        except Exception as e:
            self._logger.error(f"Error in broker relay ({game_id}, {channels}): {e}", exc_info=True)
        finally:
            self._logger.info(f"Broker relay for game_id={game_id}, channels={channels} ended.")

    async def shutdown(self) -> None:
        """
        Cancel all broker relay tasks.
        """
        self._logger.info("Shutting down broker relay...")
        async with self._lock:
            tasks_to_cancel = list(self._tasks.values())
            self._tasks.clear()

        if not tasks_to_cancel:
            return

        for task in tasks_to_cancel:
            task.cancel()
        await asyncio.gather(*tasks_to_cancel, return_exceptions=True)
        self._logger.info("All broker relays have been stopped.")
