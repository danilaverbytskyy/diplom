import socket

from django.db.models import Avg, Count, Q
from django.shortcuts import get_object_or_404, redirect, render
from django.views.decorators.http import require_http_methods
from drf_spectacular.utils import (
    OpenApiParameter,
    OpenApiTypes,
    extend_schema,
    extend_schema_view,
    inline_serializer,
)
from rest_framework import generics, serializers
from rest_framework.pagination import PageNumberPagination
from rest_framework.views import APIView

from main.models import Genre, Person, Title
from main.serializers import (
    PersonFullSerializer,
    TitleFullSerializer,
    TitleListSerializer,
)
from cache.instance import cache
from cache.mixins import CacheResponseMixin
from cache.modes import CACHE_MODE_CONFIGS


PaginatedTitleListResponseSerializer = inline_serializer(
    name='PaginatedTitleListResponse',
    fields={
        'count': serializers.IntegerField(),
        'next': serializers.CharField(allow_null=True),
        'previous': serializers.CharField(allow_null=True),
        'results': TitleListSerializer(many=True),
    },
)

TopGenreAnalyticsResponseSerializer = inline_serializer(
    name='TopGenreAnalyticsResponse',
    fields={
        'genre': serializers.CharField(),
        'titles_count': serializers.IntegerField(),
        'average_rating': serializers.FloatField(allow_null=True),
    },
    many=True,
)


PAGE_PARAMETER = OpenApiParameter(
    name='page',
    description='Номер страницы результата',
    required=False,
    type=OpenApiTypes.INT,
)

PAGE_SIZE_PARAMETER = OpenApiParameter(
    name='page_size',
    description='Количество записей на странице, не более 100',
    required=False,
    type=OpenApiTypes.INT,
)

TITLE_TYPE_PARAMETER = OpenApiParameter(
    name='title_type',
    description='Тип произведения',
    required=False,
    type=OpenApiTypes.STR,
)

ORDERING_PARAMETER = OpenApiParameter(
    name='ordering',
    description='Поле сортировки результата',
    required=False,
    type=OpenApiTypes.STR,
)


TITLE_ID_PARAMETER = OpenApiParameter(
    name='id',
    description='Внутренний идентификатор произведения',
    required=True,
    type=OpenApiTypes.INT,
    location=OpenApiParameter.PATH,
)

PERSON_ID_PARAMETER = OpenApiParameter(
    name='id',
    description='Внутренний идентификатор персоны',
    required=True,
    type=OpenApiTypes.INT,
    location=OpenApiParameter.PATH,
)


class TitlePagination(PageNumberPagination):
    page_size = 30
    page_size_query_param = 'page_size'
    max_page_size = 100


@require_http_methods(['GET', 'POST'])
def home_page(request):
    if request.method == 'POST':
        action = request.POST.get('action')

        if action == 'set_cache_mode':
            cache.set_mode(request.POST.get('cache_mode', 'off'))
            return redirect('home')

        if action == 'clear_cache':
            cache.clear()
            return redirect('home')

    context = {
        'cache_status': cache.get_status(),
        'cache_modes': [
            {'code': code, 'title': config.title}
            for code, config in CACHE_MODE_CONFIGS.items()
        ],
        'container_name': socket.gethostname(),
    }
    return render(request, 'main/main.html', context)


@extend_schema_view(
    get=extend_schema(
        tags=['Titles'],
        summary='Получение списка произведений',
        description=(
            'Возвращает список фильмов и других произведений с поддержкой '
            'фильтрации, сортировки и пагинации. Ответ кешируется.'
        ),
        parameters=[
            TITLE_TYPE_PARAMETER,
            OpenApiParameter(
                name='year',
                description='Год выпуска произведения',
                required=False,
                type=OpenApiTypes.INT,
            ),
            OpenApiParameter(
                name='is_adult',
                description='Признак взрослого контента: 0 или 1',
                required=False,
                type=OpenApiTypes.STR,
                enum=['0', '1'],
            ),
            ORDERING_PARAMETER,
            PAGE_PARAMETER,
            PAGE_SIZE_PARAMETER,
        ],
        responses=PaginatedTitleListResponseSerializer,
    ),
)
class TitleListView(CacheResponseMixin, generics.ListAPIView):
    serializer_class = TitleListSerializer
    pagination_class = TitlePagination
    cache_prefix = 'titles:list'
    cache_ttl = 300

    def get_queryset(self):
        queryset = Title.objects.all()

        title_type = self.request.query_params.get('title_type')
        year = self.request.query_params.get('year')
        is_adult = self.request.query_params.get('is_adult')
        ordering = self.request.query_params.get('ordering')

        if title_type:
            queryset = queryset.filter(title_type=title_type)

        if year:
            queryset = queryset.filter(start_year=year)

        if is_adult in ('0', '1'):
            queryset = queryset.filter(is_adult=(is_adult == '1'))

        allowed_ordering = {
            'title': 'title',
            '-title': '-title',
            'year': 'start_year',
            '-year': '-start_year',
            'runtime': 'runtime_minutes',
            '-runtime': '-runtime_minutes',
        }

        if ordering in allowed_ordering:
            queryset = queryset.order_by(allowed_ordering[ordering], 'id')
        else:
            queryset = queryset.order_by('-start_year', 'title', 'id')

        return queryset

    def build_cache_key(self) -> str:
        return f'{self.cache_prefix}:{self._query_params_key()}'

    def get_cached_data(self):
        queryset = self.get_queryset()
        page = self.paginate_queryset(queryset)

        if page is not None:
            serializer = self.get_serializer(page, many=True)
            return {
                'count': self.paginator.page.paginator.count,
                'next': self.paginator.get_next_link(),
                'previous': self.paginator.get_previous_link(),
                'results': serializer.data,
            }

        serializer = self.get_serializer(queryset, many=True)
        return serializer.data


@extend_schema_view(
    get=extend_schema(
        tags=['Titles'],
        summary='Получение топа произведений',
        description=(
            'Возвращает список произведений, отсортированных по рейтингу '
            'и количеству голосов. Ответ кешируется.'
        ),
        parameters=[
            OpenApiParameter(
                name='min_votes',
                description='Минимальное количество голосов',
                required=False,
                type=OpenApiTypes.INT,
            ),
            TITLE_TYPE_PARAMETER,
            PAGE_PARAMETER,
            PAGE_SIZE_PARAMETER,
        ],
        responses=PaginatedTitleListResponseSerializer,
    ),
)
class TopTitlesView(CacheResponseMixin, generics.ListAPIView):
    serializer_class = TitleListSerializer
    pagination_class = TitlePagination
    cache_prefix = 'titles:top'
    cache_ttl = 300

    def get_queryset(self):
        min_votes = self.request.query_params.get('min_votes', 10000)
        title_type = self.request.query_params.get('title_type')

        queryset = (
            Title.objects
            .select_related('rating')
            .prefetch_related('genres')
            .filter(rating__isnull=False, rating__num_votes__gte=min_votes)
        )

        if title_type:
            queryset = queryset.filter(title_type=title_type)

        return queryset.order_by('-rating__average_rating_tenths', '-rating__num_votes', 'id')

    def build_cache_key(self) -> str:
        return f'{self.cache_prefix}:{self._query_params_key()}'

    def get_cached_data(self):
        queryset = self.get_queryset()
        page = self.paginate_queryset(queryset)

        if page is not None:
            serializer = self.get_serializer(page, many=True)
            return {
                'count': self.paginator.page.paginator.count,
                'next': self.paginator.get_next_link(),
                'previous': self.paginator.get_previous_link(),
                'results': serializer.data,
            }

        serializer = self.get_serializer(queryset, many=True)
        return serializer.data


@extend_schema_view(
    get=extend_schema(
        tags=['Titles'],
        summary='Поиск произведений по названию',
        description=(
            'Выполняет поиск произведений по части названия. Поддерживает '
            'сортировку и пагинацию. Ответ кешируется с учётом параметров запроса.'
        ),
        parameters=[
            OpenApiParameter(
                name='q',
                description='Поисковая строка',
                required=True,
                type=OpenApiTypes.STR,
            ),
            ORDERING_PARAMETER,
            PAGE_PARAMETER,
            PAGE_SIZE_PARAMETER,
        ],
        responses=PaginatedTitleListResponseSerializer,
    ),
)
class TitleSearchView(CacheResponseMixin, generics.ListAPIView):
    serializer_class = TitleListSerializer
    pagination_class = TitlePagination
    cache_prefix = 'titles:search'
    cache_ttl = 300

    def get_queryset(self):
        query = self.request.query_params.get('q', '').strip()

        if not query:
            return Title.objects.none()

        ordering = self.request.query_params.get('ordering')

        queryset = (
            Title.objects
            .select_related('rating')
            .prefetch_related('genres')
            .filter(Q(title__icontains=query))
        )

        allowed_ordering = {
            'title': 'title',
            '-title': '-title',
            'year': 'start_year',
            '-year': '-start_year',
        }

        if ordering in allowed_ordering:
            queryset = queryset.order_by(allowed_ordering[ordering], 'id')
        else:
            queryset = queryset.order_by('-start_year', 'title', 'id')

        return queryset

    def build_cache_key(self) -> str:
        return f'{self.cache_prefix}:{self._query_params_key()}'

    def get_cached_data(self):
        queryset = self.get_queryset()
        page = self.paginate_queryset(queryset)

        if page is not None:
            serializer = self.get_serializer(page, many=True)
            return {
                'count': self.paginator.page.paginator.count,
                'next': self.paginator.get_next_link(),
                'previous': self.paginator.get_previous_link(),
                'results': serializer.data,
            }

        serializer = self.get_serializer(queryset, many=True)
        return serializer.data


@extend_schema_view(
    get=extend_schema(
        tags=['Titles'],
        summary='Получение краткой информации о произведении',
        description='Возвращает краткую информацию о произведении по внутреннему идентификатору.',
        parameters=[TITLE_ID_PARAMETER],
        responses=TitleListSerializer,
    ),
)
class TitleDetailView(CacheResponseMixin, APIView):
    cache_prefix = 'title:detail'
    cache_ttl = 600

    def build_cache_key(self) -> str:
        return f'{self.cache_prefix}:{self.kwargs["id"]}'

    def get_cached_data(self):
        obj = get_object_or_404(Title, id=self.kwargs['id'])
        serializer = TitleListSerializer(obj)
        return serializer.data


@extend_schema_view(
    get=extend_schema(
        tags=['Titles'],
        summary='Получение полной информации о произведении',
        description=(
            'Возвращает подробную информацию о произведении, включая рейтинг, '
            'жанры, съёмочную группу и основных участников.'
        ),
        parameters=[TITLE_ID_PARAMETER],
        responses=TitleFullSerializer,
    ),
)
class TitleFullDetailView(CacheResponseMixin, APIView):
    cache_prefix = 'title:full'
    cache_ttl = 600

    def build_cache_key(self) -> str:
        return f'{self.cache_prefix}:{self.kwargs["id"]}'

    def get_cached_data(self):
        obj = get_object_or_404(
            Title.objects
            .select_related('rating')
            .prefetch_related(
                'genres',
                'crew_members__person',
                'principals__person',
            ),
            id=self.kwargs['id'],
        )
        serializer = TitleFullSerializer(obj)
        return serializer.data


@extend_schema_view(
    get=extend_schema(
        tags=['Titles'],
        summary='Подборка произведений с расширенной фильтрацией',
        description=(
            'Возвращает список произведений с фильтрацией по типу, жанру, '
            'диапазону лет, количеству голосов и сортировке.'
        ),
        parameters=[
            TITLE_TYPE_PARAMETER,
            OpenApiParameter(
                name='genre',
                description='Название жанра',
                required=False,
                type=OpenApiTypes.STR,
            ),
            OpenApiParameter(
                name='year_from',
                description='Начальный год выпуска',
                required=False,
                type=OpenApiTypes.INT,
            ),
            OpenApiParameter(
                name='year_to',
                description='Конечный год выпуска',
                required=False,
                type=OpenApiTypes.INT,
            ),
            OpenApiParameter(
                name='min_votes',
                description='Минимальное количество голосов',
                required=False,
                type=OpenApiTypes.INT,
            ),
            ORDERING_PARAMETER,
            PAGE_PARAMETER,
            PAGE_SIZE_PARAMETER,
        ],
        responses=PaginatedTitleListResponseSerializer,
    ),
)
class TitleDiscoverView(CacheResponseMixin, generics.ListAPIView):
    serializer_class = TitleListSerializer
    pagination_class = TitlePagination
    cache_prefix = 'titles:discover'
    cache_ttl = 300

    def get_queryset(self):
        queryset = (
            Title.objects
            .select_related('rating')
            .prefetch_related('genres')
            .all()
        )

        title_type = self.request.query_params.get('title_type')
        genre = self.request.query_params.get('genre')
        year_from = self.request.query_params.get('year_from')
        year_to = self.request.query_params.get('year_to')
        min_votes = self.request.query_params.get('min_votes')
        ordering = self.request.query_params.get('ordering')

        if title_type:
            queryset = queryset.filter(title_type=title_type)

        if genre:
            queryset = queryset.filter(genres__name__iexact=genre)

        if year_from:
            queryset = queryset.filter(start_year__gte=year_from)

        if year_to:
            queryset = queryset.filter(start_year__lte=year_to)

        if min_votes:
            queryset = queryset.filter(rating__num_votes__gte=min_votes)

        allowed_ordering = {
            'title': 'title',
            '-title': '-title',
            'year': 'start_year',
            '-year': '-start_year',
            'rating': 'rating__average_rating_tenths',
            '-rating': '-rating__average_rating_tenths',
            'votes': 'rating__num_votes',
            '-votes': '-rating__num_votes',
        }

        if ordering in allowed_ordering:
            queryset = queryset.order_by(allowed_ordering[ordering], 'id')
        else:
            queryset = queryset.order_by('-rating__average_rating_tenths', '-rating__num_votes', 'id')

        return queryset.distinct()

    def build_cache_key(self) -> str:
        return f'{self.cache_prefix}:{self._query_params_key()}'

    def get_cached_data(self):
        queryset = self.get_queryset()
        page = self.paginate_queryset(queryset)

        if page is not None:
            serializer = self.get_serializer(page, many=True)
            return {
                'count': self.paginator.page.paginator.count,
                'next': self.paginator.get_next_link(),
                'previous': self.paginator.get_previous_link(),
                'results': serializer.data,
            }

        serializer = self.get_serializer(queryset, many=True)
        return serializer.data


@extend_schema_view(
    get=extend_schema(
        tags=['Persons'],
        summary='Получение полной информации о персоне',
        description=(
            'Возвращает сведения о персоне и связанных с ней произведениях, '
            'в которых она участвовала как основной участник или член съёмочной группы.'
        ),
        parameters=[PERSON_ID_PARAMETER],
        responses=PersonFullSerializer,
    ),
)
class PersonFullDetailView(CacheResponseMixin, APIView):
    cache_prefix = 'person:full'
    cache_ttl = 600

    def build_cache_key(self) -> str:
        return f'{self.cache_prefix}:{self.kwargs["id"]}'

    def get_cached_data(self):
        obj = get_object_or_404(
            Person.objects.prefetch_related(
                'principal_titles__title__rating',
                'crew_titles__title__rating',
            ),
            id=self.kwargs['id'],
        )
        serializer = PersonFullSerializer(obj)
        return serializer.data


@extend_schema_view(
    get=extend_schema(
        tags=['Analytics'],
        summary='Получение аналитики по жанрам',
        description=(
            'Возвращает список жанров с количеством связанных произведений '
            'и средним рейтингом. Endpoint используется для демонстрации '
            'кеширования более затратных аналитических запросов.'
        ),
        responses=TopGenreAnalyticsResponseSerializer,
    ),
)
class TopGenresAnalyticsView(CacheResponseMixin, APIView):
    cache_prefix = 'analytics:top-genres'
    cache_ttl = 900

    def build_cache_key(self) -> str:
        return f'{self.cache_prefix}:{self._query_params_key()}'

    def get_cached_data(self):
        queryset = (
            Genre.objects
            .annotate(
                titles_count=Count('titles', distinct=True),
                avg_rating_tenths=Avg('titles__rating__average_rating_tenths'),
            )
            .order_by('-titles_count', 'name')[:50]
        )

        data = []
        for genre in queryset:
            avg_rating = None
            if genre.avg_rating_tenths is not None:
                avg_rating = round(genre.avg_rating_tenths / 10, 2)

            data.append({
                'genre': genre.name,
                'titles_count': genre.titles_count,
                'average_rating': avg_rating,
            })

        return data
