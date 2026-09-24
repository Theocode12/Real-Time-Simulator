from redis import Redis

from utils.load_config import load_config


def get_redis_client() -> Redis:
    config = load_config()

    # Prefer a full URL when provided (carries password/TLS/db in one place).
    redis_url = config.get("app", "redisUrl", fallback="").strip()
    if redis_url:
        return Redis.from_url(redis_url, decode_responses=True)

    password = config.get("app", "redisPassword", fallback="")
    username = config.get("app", "redisUsername", fallback="default")
    return Redis(
        host=config.get("app", "redisHost", fallback="localhost"),
        port=config.getint("app", "redisPort", fallback=6379),
        username=username,
        password=password,
        decode_responses=True,
    )
