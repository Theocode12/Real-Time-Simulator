from __future__ import annotations

import logging
from configparser import ConfigParser

from socketio import AsyncServer

from app.broker.message_broker import MessageBroker
from app.commentary.manager import CommentaryManager
from app.core.ws_auth import AuthService
from app.handlers.broker_relay import BrokerRelay
from app.scheduler.manager import SchedulerManager
from app.services.support_service import SupportService
from app.websockets_api.namespaces.game_namespace import GameNamespace
from app.websockets_api.namespaces.message_namespace import MessageNamespace
from app.websockets_api.namespaces.support_namespace import SupportNamespace
from app.websockets_api.routes.router import Router


def bulid_socketio_server_context(
    sio: AsyncServer,
    config: ConfigParser,
    logger: logging.Logger,
) -> SocketIOServerContext:
    from app.broker.message_broker_factory import get_message_broker
    from app.core.ws_auth import AuthService
    from app.dependencies import get_redis_client
    from app.handlers.broker_relay import BrokerRelay
    from app.infra.support_store import SupportStore
    from app.scheduler.manager import SchedulerManager
    from app.services.support_service import SupportService
    from app.websockets_api.routes.router import Router

    broker = get_message_broker(config, logger)
    auth = AuthService()
    router = Router(logger=logger)
    commentary_manager = CommentaryManager(broker, config=config, logger=logger)
    scheduler_manager = SchedulerManager(
        broker,
        config=config,
        logger=logger,
        commentary_manager=commentary_manager,
    )
    broker_relay = BrokerRelay(sio, broker, logger)

    support_store = SupportStore(
        get_redis_client(),
        ttl_seconds=config.getint("support", "ttlSeconds", fallback=43200),
        live_registry_prefix=config.get("liveGameRegistry", "redisKeyPrefix", fallback="live:game"),
        logger=logger,
    )
    support_service = SupportService(support_store, logger=logger)

    return SocketIOServerContext(
        sio=sio,
        broker=broker,
        auth=auth,
        router=router,
        scheduler_manager=scheduler_manager,
        broker_relay=broker_relay,
        support=support_service,
        commentary_manager=commentary_manager,
        support_enabled=config.getboolean("support", "enabled", fallback=True),
    )


class SocketIOServerContext:
    def __init__(
        self,
        sio: AsyncServer,
        broker: MessageBroker,
        auth: AuthService,
        router: Router,
        scheduler_manager: SchedulerManager,
        broker_relay: BrokerRelay,
        support: SupportService,
        commentary_manager: CommentaryManager | None = None,
        support_enabled: bool = True,
    ) -> None:
        from app.core.context import AppContext

        self.sio = sio
        self.broker = broker
        self.scheduler_manager = scheduler_manager
        self.broker_relay = broker_relay
        self.commentary_manager = commentary_manager
        self.support_enabled = support_enabled

        self.context = AppContext(
            sio=sio,
            broker=broker,
            auth=auth,
            scheduler_manager=scheduler_manager,
            router=router,
            broker_relay=self.broker_relay,
            support=support,
            commentary_manager=commentary_manager,
        )
        router.load_routes()  # move to main later

    def register(self) -> None:
        self.context.sio.register_namespace(GameNamespace("/game", self.context))
        self.context.sio.register_namespace(MessageNamespace("/messages", self.context))
        if self.support_enabled:
            self.context.sio.register_namespace(SupportNamespace("/support", self.context))

    def get_scheduler_manager(self) -> SchedulerManager:
        return self.scheduler_manager

    async def shutdown(self) -> None:
        """
        Gracefully shut down all websocket-related resources.
        """

        if self.commentary_manager is not None:
            await self.commentary_manager.shutdown()
        await self.scheduler_manager.shutdown()
        await self.broker.shutdown()
        await self.broker_relay.shutdown()
        await self.sio.shutdown()
