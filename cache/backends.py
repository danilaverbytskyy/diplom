import logging
import pickle
import time
from typing import Any

from redis import Redis
from redis.exceptions import RedisError

logger = logging.getLogger(__name__)


class LocalCacheBackend:
    def __init__(self, enabled: bool = True) -> None:
        self.enabled = enabled
        self._store: dict[str, tuple[Any, float | None]] = {}

    def get(self, key: str) -> Any | None:
        if not self.enabled:
            return None

        item = self._store.get(key)
        if item is None:
            return None

        value, expires_at = item

        if expires_at is not None and time.time() > expires_at:
            self.delete(key)
            return None

        return value

    def set(self, key: str, value: Any, ttl: int | None = None) -> None:
        if not self.enabled:
            return

        expires_at = time.time() + ttl if ttl else None
        self._store[key] = (value, expires_at)

    def delete(self, key: str) -> None:
        self._store.pop(key, None)

    def clear(self) -> None:
        # clear() должен работать даже когда local cache выключен: это нужно,
        # чтобы сбросить память процесса после переключения общего режима кеша.
        self._store.clear()

    def size(self) -> int:
        return len(self._store)


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

        return pickle.loads(raw)

    def set(self, key: str, value: Any, ttl: int | None = None) -> None:
        if not self.enabled:
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
