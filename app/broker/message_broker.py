from __future__ import annotations

import configparser
import logging
from abc import ABC, abstractmethod
from collections.abc import AsyncGenerator
from enum import StrEnum, auto
from typing import Any

from app.shared.enums.broker_channels import BrokerChannels
from utils.load_config import load_config
from utils.logger import get_logger


class OverflowPolicy(StrEnum):
    """
    What a broker should do when a subscriber's queue is full.

    - ``BLOCK``: wait for space (may throttle the producer).
    - ``DROP_NEW``: discard the incoming message for that subscriber.
    - ``DROP_OLD``: evict the oldest queued message to make room (keep freshest).
    """

    BLOCK = auto()
    DROP_NEW = auto()
    DROP_OLD = auto()


class MessageBroker(ABC):
    def __init__(
        self,
        config: configparser.ConfigParser | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self.logger = logger or get_logger()
        self.config = config or load_config()

    @abstractmethod
    async def publish(self, game_id: str, channel: BrokerChannels, message: Any) -> int:
        """Publish message to specific game/channel"""
        pass

    @abstractmethod
    async def subscribe(
        self,
        game_id: str,
        channels: BrokerChannels | list[BrokerChannels],
        *,
        policy: OverflowPolicy = OverflowPolicy.BLOCK,
        maxsize: int | None = None,
    ) -> AsyncGenerator[Any, None]:
        """Subscribe to game/channel messages"""

        async def generator() -> AsyncGenerator[Any, None]:
            yield

        return generator()

    @abstractmethod
    async def shutdown(self) -> None:
        """Cleanup resources"""
        pass
