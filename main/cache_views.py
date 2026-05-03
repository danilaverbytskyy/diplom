from rest_framework import status
from rest_framework.decorators import api_view
from rest_framework.response import Response

from cache.instance import cache


ALLOWED_CACHE_MODES = ('off', 'local', 'redis', 'multi')


@api_view(['GET'])
def cache_status(request):
    """Возвращает текущий статус подсистемы кеширования."""

    return Response(cache.get_status())


@api_view(['POST'])
def cache_mode(request):
    """Переключает режим кеширования."""

    mode = request.data.get('mode')

    if mode not in ALLOWED_CACHE_MODES:
        return Response(
            {
                'error': 'Invalid cache mode',
                'allowed_modes': ALLOWED_CACHE_MODES,
            },
            status=status.HTTP_400_BAD_REQUEST,
        )

    try:
        cache.set_mode(mode)
    except Exception as exc:
        return Response(
            {
                'error': 'Failed to set cache mode',
                'detail': str(exc),
            },
            status=status.HTTP_503_SERVICE_UNAVAILABLE,
        )

    return Response(cache.get_status())


@api_view(['POST'])
def cache_clear(request):
    """Очищает кеш и возвращает обновлённый статус."""

    try:
        cache.clear()
    except Exception as exc:
        return Response(
            {
                'error': 'Failed to clear cache',
                'detail': str(exc),
            },
            status=status.HTTP_503_SERVICE_UNAVAILABLE,
        )

    return Response(cache.get_status())