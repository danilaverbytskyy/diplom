from rest_framework.response import Response

from cache.instance import cache


class CacheResponseMixin:
    cache_ttl = 300
    cache_prefix = None

    def build_cache_key(self) -> str:
        raise NotImplementedError

    def get_cached_data(self):
        raise NotImplementedError

    def get_cache_ttl(self) -> int:
        return self.cache_ttl

    def get(self, request, *args, **kwargs):
        cache_key = self.build_cache_key()

        data = cache.get_or_set(
            key=cache_key,
            factory=self.get_cached_data,
            ttl=self.get_cache_ttl(),
        )
        return Response(data)

    def _query_params_key(self) -> str:
        params = self.request.query_params
        parts = [f'{key}={params.get(key)}' for key in sorted(params.keys())]
        return '|'.join(parts) if parts else 'default'