import pickle
import time
from typing import Any

from redis import Redis


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
        if not self.enabled:
            return

        self._store.pop(key, None)

    def clear(self) -> None:
        if not self.enabled:
            return

        self._store.clear()


class RedisCacheBackend:
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
        return f'{self.prefix}:{key}'

    def get(self, key: str) -> Any | None:
        if not self.enabled:
            return None

        raw = self.redis_client.get(self._build_key(key))
        if raw is None:
            return None

        return pickle.loads(raw)

    def set(self, key: str, value: Any, ttl: int | None = None) -> None:
        if not self.enabled:
            return

        full_key = self._build_key(key)
        payload = pickle.dumps(value)

        if ttl:
            self.redis_client.setex(full_key, ttl, payload)
        else:
            self.redis_client.set(full_key, payload)

    def delete(self, key: str) -> None:
        if not self.enabled:
            return

        self.redis_client.delete(self._build_key(key))

    def clear(self) -> None:
        if not self.enabled:
            return

        pattern = f'{self.prefix}:*'
        cursor = 0

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