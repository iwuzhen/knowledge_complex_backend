from fastapi import FastAPI
from fastapi_cache import FastAPICache
from fastapi_cache.backends.redis import RedisBackend
from redis import asyncio as aioRedis
from redis.asyncio import ConnectionPool

from knowledge_complex_backend.settings import settings


def init_redis(app: FastAPI) -> None:  # pragma: no cover
    """
    Creates connection pool for redis.

    :param app: current fastapi application.
    """
    app.state.redis_pool = ConnectionPool.from_url(
        str(settings.redis_url),
    )
    # import logging
    # logging.info(str(settings.redis_url))
    # cache
    redis = aioRedis.from_url(
        str(settings.redis_url), encoding="utf8", decode_responses=True
    )
    FastAPICache.init(RedisBackend(redis), prefix="fastapi-cache")


async def shutdown_redis(app: FastAPI) -> None:  # pragma: no cover
    """
    Closes redis connection pool.

    :param app: current FastAPI app.
    """
    await app.state.redis_pool.disconnect()
