import logging
from dataclasses import dataclass

from redis import Redis
from redis.exceptions import RedisError

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CacheModeConfig:
    enabled: bool
    local_enabled: bool
    redis_enabled: bool
    title: str


CACHE_MODE_CONFIGS: dict[str, CacheModeConfig] = {
    'off': CacheModeConfig(False, False, False, 'Без кеша'),
    'local': CacheModeConfig(True, True, False, 'Только Local'),
    'redis': CacheModeConfig(True, False, True, 'Только Redis'),
    'multi': CacheModeConfig(True, True, True, 'Local + Redis'),
}


class RedisCacheModeStorage:
    """Хранилище общего режима кеширования для всех контейнеров.

    mode_key хранит выбранный режим: off/local/redis/multi.
    version_key хранит номер версии. При смене режима или полном сбросе кеша
    версия увеличивается, и каждый контейнер очищает свой локальный in-memory cache.
    """

    def __init__(
        self,
        redis_client: Redis,
        mode_key: str,
        version_key: str,
        default_mode: str = 'off',
    ) -> None:
        self.redis_client = redis_client
        self.mode_key = mode_key
        self.version_key = version_key
        self.default_mode = self.normalize_mode(default_mode)

    def get_mode(self) -> str:
        try:
            raw = self.redis_client.get(self.mode_key)
        except RedisError:
            logger.exception('Failed to read cache mode from Redis')
            return self.default_mode

        if raw is None:
            return self.default_mode

        if isinstance(raw, bytes):
            raw = raw.decode('utf-8')

        return self.normalize_mode(str(raw))

    def set_mode(self, mode: str) -> str:
        mode = self.normalize_mode(mode)

        try:
            pipe = self.redis_client.pipeline()
            pipe.set(self.mode_key, mode)
            pipe.incr(self.version_key)
            pipe.execute()
        except RedisError:
            logger.exception('Failed to save cache mode to Redis')
            raise

        return mode

    def get_version(self) -> int:
        try:
            raw = self.redis_client.get(self.version_key)
        except RedisError:
            logger.exception('Failed to read cache version from Redis')
            return 0

        if raw is None:
            return 0

        if isinstance(raw, bytes):
            raw = raw.decode('utf-8')

        try:
            return int(raw)
        except (TypeError, ValueError):
            return 0

    def bump_version(self) -> int:
        try:
            return int(self.redis_client.incr(self.version_key))
        except RedisError:
            logger.exception('Failed to bump cache version in Redis')
            raise

    def normalize_mode(self, mode: str | None) -> str:
        if mode in CACHE_MODE_CONFIGS:
            return str(mode)

        return self.default_mode if hasattr(self, 'default_mode') else 'off'

    # Обратная совместимость со старым интерфейсом.
    def get(self) -> str:
        return self.get_mode()

    def set(self, mode: str) -> None:
        self.set_mode(mode)
