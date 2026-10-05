from __future__ import annotations


def _resolve_redis_url() -> str:
    """Single source of truth: reuse the backend ``[app] redisUrl``.

    Falls back to localhost so fresh clones/tests without a config.ini
    keep working.
    """
    try:
        from utils.load_config import load_config

        url = load_config().get("app", "redisUrl", fallback="").strip()
        if url:
            return url
    except Exception:
        pass
    return "redis://localhost:6379"


CONFIG = {
    "app": {
        "redisUrl": _resolve_redis_url(),
        "enableStorage": True,
        "storageType": "redis",
        "ttl": 43200,
        "fileStorageDir": "game_data",
    },
    "logging": {
        "enabled": True,
        "level": "DEBUG",
        "consoleLogs": False,
        "serverLogs": False,
        "serverLogFile": "logs/server.log",
        "maxFileSize": 10485760,
        "backupCount": 5,
    },
}
