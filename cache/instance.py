from django.conf import settings
from redis import Redis

from .backends import LocalCacheBackend, RedisCacheBackend
from .service import MultiLevelCache


redis_client = Redis.from_url(settings.REDIS_URL)

cache = MultiLevelCache(
    local_cache=LocalCacheBackend(),
    redis_cache=RedisCacheBackend(
        redis_client=redis_client,
        prefix=settings.CACHE_PREFIX,
    ),
)