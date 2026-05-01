import logging
import pickle
import time
from collections import OrderedDict
from threading import RLock
from typing import Any

from redis import Redis
from redis.exceptions import RedisError

logger = logging.getLogger(__name__)


class LocalCacheBackend:
    def __init__(
        self,
        enabled: bool = True,
        max_size: int = 1024,
    ) -> None:
        self.enabled = enabled
        self.max_size = max_size
        self._store: OrderedDict[str, tuple[Any, float | None]] = OrderedDict()
        self._lock = RLock()

    def get(self, key: str) -> Any | None:
        if not self.enabled:
            return None

        with self._lock:
            item = self._store.get(key)
            if item is None:
                return None

            value, expires_at = item

            if expires_at is not None and time.time() > expires_at:
                self._store.pop(key, None)
                return None

            self._store.move_to_end(key)
            return value

    def set(self, key: str, value: Any, ttl: int | None = None) -> None:
        if not self.enabled:
            return

        if ttl is not None and ttl <= 0:
            self.delete(key)
            return

        expires_at = time.time() + ttl if ttl else None

        with self._lock:
            self._store[key] = (value, expires_at)
            self._store.move_to_end(key)
            self._evict_extra_items()

    def delete(self, key: str) -> None:
        with self._lock:
            self._store.pop(key, None)

    def clear(self) -> None:
        # clear() должен работать даже когда local cache выключен: это нужно,
        # чтобы сбросить память процесса после переключения общего режима кеша.
        with self._lock:
            self._store.clear()

    def size(self) -> int:
        with self._lock:
            return len(self._store)

    def _evict_extra_items(self) -> None:
        if self.max_size <= 0:
            self._store.clear()
            return

        while len(self._store) > self.max_size:
            self._store.popitem(last=False)


class RedisCacheBackend:
    """Redis backend для данных приложения.

    Данные намеренно кладутся в namespace ``<prefix>:data:*``. Ключи управления
    режимом кеширования используют другой namespace и не удаляются при clear().
    """

    def __init__(
        self,
        redis_client: Redis,
        prefix: str = 'app',
        enabled: bool = True,
    ) -> None:
        self.redis_client = redis_client
        self.prefix = prefix
        self.enabled = enabled

    def _build_key(self, key: str) -> str:
        return f'{self.prefix}:data:{key}'

    def get(self, key: str) -> Any | None:
        if not self.enabled:
            return None

        try:
            raw = self.redis_client.get(self._build_key(key))
        except RedisError:
            logger.exception('Failed to read cached value from Redis')
            return None

        if raw is None:
            return None

        try:
            return pickle.loads(raw)
        except (pickle.PickleError, EOFError, AttributeError, ValueError, TypeError):
            logger.exception('Failed to deserialize cached value from Redis')
            self.delete(key)
            return None

    def set(self, key: str, value: Any, ttl: int | None = None) -> None:
        if not self.enabled:
            return

        if ttl is not None and ttl <= 0:
            self.delete(key)
            return

        full_key = self._build_key(key)
        payload = pickle.dumps(value)

        try:
            if ttl:
                self.redis_client.setex(full_key, ttl, payload)
            else:
                self.redis_client.set(full_key, payload)
        except RedisError:
            logger.exception('Failed to save cached value to Redis')

    def delete(self, key: str) -> None:
        try:
            self.redis_client.delete(self._build_key(key))
        except RedisError:
            logger.exception('Failed to delete cached value from Redis')

    def clear(self) -> None:
        # Сброс должен работать независимо от текущего режима кеша.
        pattern = f'{self.prefix}:data:*'
        cursor = 0

        try:
            while True:
                cursor, keys = self.redis_client.scan(
                    cursor=cursor,
                    match=pattern,
                    count=100,
                )
                if keys:
                    self.redis_client.delete(*keys)

                if cursor == 0:
                    break
        except RedisError:
            logger.exception('Failed to clear Redis cache')
