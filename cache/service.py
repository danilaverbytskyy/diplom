from typing import Any, Callable

from .interfaces import CacheInterface


class MultiLevelCache(CacheInterface):
    def __init__(
        self,
        local_cache,
        redis_cache,
        enabled: bool = True,
    ) -> None:
        self.local_cache = local_cache
        self.redis_cache = redis_cache
        self.enabled = enabled

    def get(self, key: str, ttl: int | None = None) -> Any | None:
        if not self.enabled:
            return None

        value = self.local_cache.get(key)
        if value is not None:
            return value

        value = self.redis_cache.get(key)
        if value is not None:
            self.local_cache.set(key, value, ttl)
            return value

        return None

    def set(self, key: str, value: Any, ttl: int | None = None) -> None:
        if not self.enabled:
            return

        self.local_cache.set(key, value, ttl)
        self.redis_cache.set(key, value, ttl)

    def delete(self, key: str) -> None:
        if not self.enabled:
            return

        self.local_cache.delete(key)
        self.redis_cache.delete(key)

    def clear(self) -> None:
        self.local_cache.clear()
        self.redis_cache.clear()

    def get_or_set(
        self,
        key: str,
        factory: Callable[[], Any],
        ttl: int | None = None,
    ) -> Any:
        if not self.enabled:
            return factory()

        value = self.get(key, ttl)
        if value is not None:
            return value

        value = factory()
        if value is not None:
            self.set(key, value, ttl)

        return value

    def configure(
        self,
        enabled: bool,
        local_enabled: bool,
        redis_enabled: bool,
    ) -> None:
        self.enabled = enabled
        self.local_cache.enabled = local_enabled
        self.redis_cache.enabled = redis_enabled

    def get_status(self) -> dict[str, bool | str]:
        if not self.enabled:
            mode = 'Без кеша'
        elif self.local_cache.enabled and self.redis_cache.enabled:
            mode = 'Local + Redis'
        elif self.local_cache.enabled and not self.redis_cache.enabled:
            mode = 'Только Local'
        elif not self.local_cache.enabled and self.redis_cache.enabled:
            mode = 'Только Redis'
        else:
            mode = 'Без кеша'

        return {
            'cache_enabled': self.enabled,
            'local_cache_enabled': self.local_cache.enabled,
            'redis_cache_enabled': self.redis_cache.enabled,
            'mode': mode,
        }
