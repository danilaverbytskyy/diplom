from django.conf import settings
from redis import Redis

from .backends import LocalCacheBackend, RedisCacheBackend
from .modes import RedisCacheModeStorage
from .service import MultiLevelCache


redis_client = Redis.from_url(
    settings.REDIS_URL,
    socket_connect_timeout=1,
    socket_timeout=1,
)

mode_storage = RedisCacheModeStorage(
    redis_client=redis_client,
    mode_key=settings.CACHE_MODE_KEY,
    version_key=settings.CACHE_VERSION_KEY,
    default_mode=settings.CACHE_MODE,
)

cache = MultiLevelCache(
    local_cache=LocalCacheBackend(enabled=False),
    redis_cache=RedisCacheBackend(
        redis_client=redis_client,
        prefix=settings.CACHE_PREFIX,
        enabled=False,
    ),
    mode_storage=mode_storage,
    default_mode=settings.CACHE_MODE,
)
