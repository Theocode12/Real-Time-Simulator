import logging
import os
from configparser import ConfigParser
from functools import lru_cache
from pathlib import Path
from typing import Annotated, cast

import redis.asyncio as redis
from fastapi import Depends, Header, HTTPException, Request

from app.infra.music_file_store import MusicFileStore
from app.infra.music_redis_store import MusicRedisStore
from app.services.music_service import MusicService
from app.services.support_service import SupportService
from db.redis_storage import RedisStorageSingleton
from utils.load_config import load_config
from utils.logger import get_logger

logger = get_logger(__name__)

# Singletons
redis_store: RedisStorageSingleton | None = None
music_service_instance: MusicService | None = None


@lru_cache
def get_app_config() -> ConfigParser:
    try:
        config = load_config()
    except Exception as e:
        logger.exception(f"Error loading config: {e}")
        raise
    return config


async def init_redis() -> None:
    global redis_store
    config = get_app_config()
    redis_store = RedisStorageSingleton(config)
    await redis_store.connect()


async def close_redis() -> None:
    if redis_store:
        await redis_store.close()


def get_redis_client() -> redis.Redis:
    if not redis_store:
        raise RuntimeError("Redis not initialized")
    return redis_store.get_client()


def get_music_service() -> MusicService:
    global music_service_instance
    config = load_config()
    if music_service_instance:
        return music_service_instance

    redis_client = get_redis_client()
    file_store = MusicFileStore(Path(config.get("media", "root_dir")), Path(config.get("media", "music_store_path")))
    redis_store_obj = MusicRedisStore(redis_client)

    music_service_instance = MusicService(file_store, redis_store_obj)
    return music_service_instance


def get_support_service(request: Request) -> SupportService:
    """
    Return the shared SupportService instance created with the Socket.IO context.

    Keeping a single instance ensures the REST API and the websocket namespace
    read and write the exact same support state.
    """
    sio_context = getattr(request.app.state, "sio_context", None)
    support_service = getattr(sio_context.context, "support", None) if sio_context else None

    if support_service is None:
        logger.error("Support service not available on app state")
        raise HTTPException(status_code=503, detail="Support service unavailable")

    return cast(SupportService, support_service)


def get_app_logger(
    config: Annotated[ConfigParser, Depends(get_app_config)],
) -> logging.Logger:
    try:
        app_logger = get_logger(__name__, config)
    except Exception as e:
        logger.exception(f"Error creating logger: {e}")
        raise

    return app_logger


async def require_secret(x_secret: str | None = Header(None)) -> None:
    """
    Dependency that checks the ``X-Secret`` header against
    the ``API_SECRET`` environment variable.

    Returns 404 when the secret is missing or wrong — the endpoint
    simply doesn't exist for unauthenticated callers.
    """
    expected = os.getenv("API_SECRET")
    if not expected:
        logger.error("API_SECRET not set")
        raise HTTPException(status_code=404, detail="Not found")

    if x_secret != expected:
        raise HTTPException(status_code=404, detail="Not found")
