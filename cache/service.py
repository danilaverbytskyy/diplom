from threading import Lock, RLock
from typing import Any, Callable

from .interfaces import CacheInterface
from .modes import CACHE_MODE_CONFIGS, RedisCacheModeStorage


class MultiLevelCache(CacheInterface):
    def __init__(
        self,
        local_cache,
        redis_cache,
        mode_storage: RedisCacheModeStorage | None = None,
        default_mode: str = 'off',
        use_locks: bool = True,
    ) -> None:
        self.local_cache = local_cache
        self.redis_cache = redis_cache
        self.mode_storage = mode_storage
        self.current_mode = self._normalize_mode(default_mode)
        self.use_locks = use_locks
        self._last_seen_version: int | None = None
        self._locks: dict[str, Lock] = {}
        self._locks_guard = RLock()
        self._apply_mode(self.current_mode)

    @property
    def enabled(self) -> bool:
        return CACHE_MODE_CONFIGS[self.current_mode].enabled

    def _normalize_mode(self, mode: str | None) -> str:
        return mode if mode in CACHE_MODE_CONFIGS else 'off'

    def _apply_mode(self, mode: str) -> None:
        mode = self._normalize_mode(mode)
        config = CACHE_MODE_CONFIGS[mode]

        self.current_mode = mode
        self.local_cache.enabled = config.local_enabled
        self.redis_cache.enabled = config.redis_enabled

    def _refresh_configuration(self) -> None:
        """Подтягивает общий режим и version-token из Redis.

        Метод вызывается перед каждой операцией кеша, поэтому переключение кнопкой
        в одном контейнере становится видимым для остальных контейнеров на
        следующем запросе.
        """
        if self.mode_storage is None:
            return

        mode = self.mode_storage.get_mode()
        version = self.mode_storage.get_version()

        if self._last_seen_version is None:
            self._last_seen_version = version
        elif version != self._last_seen_version:
            self.local_cache.clear()
            self._last_seen_version = version

        self._apply_mode(mode)

    def get(self, key: str, ttl: int | None = None) -> Any | None:
        self._refresh_configuration()

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
        self._refresh_configuration()

        if not self.enabled:
            return

        self.local_cache.set(key, value, ttl)
        self.redis_cache.set(key, value, ttl)

    def delete(self, key: str) -> None:
        self._refresh_configuration()
        self.local_cache.delete(key)
        self.redis_cache.delete(key)

    def clear(self) -> None:
        """Очищает кеш во всех backend-ах и инвалидирует local cache во всех контейнерах."""
        self.local_cache.clear()
        self.redis_cache.clear()

        if self.mode_storage is not None:
            self._last_seen_version = self.mode_storage.bump_version()

    def get_or_set(
        self,
        key: str,
        factory: Callable[[], Any],
        ttl: int | None = None,
    ) -> Any:
        self._refresh_configuration()

        if not self.enabled:
            return factory()

        value = self.get(key, ttl)
        if value is not None:
            return value

        if not self.use_locks:
            return self._calculate_and_store(key, factory, ttl)

        lock = self._get_lock(key)

        with lock:
            value = self.get(key, ttl)
            if value is not None:
                return value

            return self._calculate_and_store(key, factory, ttl)

    def set_mode(self, mode: str) -> str:
        mode = self._normalize_mode(mode)

        if self.mode_storage is not None:
            mode = self.mode_storage.set_mode(mode)
            self._last_seen_version = self.mode_storage.get_version()

        self.local_cache.clear()
        self._apply_mode(mode)
        return mode

    def configure(
        self,
        enabled: bool,
        local_enabled: bool,
        redis_enabled: bool,
    ) -> None:
        """Совместимость со старым API configure(...)."""
        if not enabled:
            mode = 'off'
        elif local_enabled and redis_enabled:
            mode = 'multi'
        elif local_enabled:
            mode = 'local'
        elif redis_enabled:
            mode = 'redis'
        else:
            mode = 'off'

        self.set_mode(mode)

    def get_status(self) -> dict[str, bool | int | str]:
        self._refresh_configuration()
        config = CACHE_MODE_CONFIGS[self.current_mode]

        return {
            'mode_code': self.current_mode,
            'mode': config.title,
            'cache_enabled': config.enabled,
            'local_cache_enabled': config.local_enabled,
            'redis_cache_enabled': config.redis_enabled,
            'local_cache_size': self.local_cache.size(),
            'version': self._last_seen_version or 0,
        }

    def _calculate_and_store(
        self,
        key: str,
        factory: Callable[[], Any],
        ttl: int | None,
    ) -> Any:
        value = factory()
        if value is not None:
            self.set(key, value, ttl)

        return value

    def _get_lock(self, key: str) -> Lock:
        with self._locks_guard:
            lock = self._locks.get(key)
            if lock is None:
                lock = Lock()
                self._locks[key] = lock

            return lock
