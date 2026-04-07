from django.db.models import Avg, Count, Q
from django.shortcuts import get_object_or_404
from rest_framework import generics
from rest_framework.pagination import PageNumberPagination
from rest_framework.response import Response
from rest_framework.views import APIView
from django.shortcuts import redirect, render
from django.views.decorators.http import require_http_methods

from main.models import Crew, Genre, Person, Principal, Title
from main.serializers import (
    PersonFullSerializer,
    TitleFullSerializer,
    TitleListSerializer,
)
from cache.instance import cache


class TitlePagination(PageNumberPagination):
    page_size = 30
    page_size_query_param = 'page_size'
    max_page_size = 100


@require_http_methods(['GET', 'POST'])
def home_page(request):
    if request.method == 'POST':
        action = request.POST.get('action')

        if action == 'set_cache_mode':
            mode = request.POST.get('cache_mode')

            if mode == 'off':
                cache.configure(
                    enabled=False,
                    local_enabled=False,
                    redis_enabled=False,
                )
            elif mode == 'local':
                cache.configure(
                    enabled=True,
                    local_enabled=True,
                    redis_enabled=False,
                )
            elif mode == 'redis':
                cache.configure(
                    enabled=True,
                    local_enabled=False,
                    redis_enabled=True,
                )
            elif mode == 'multi':
                cache.configure(
                    enabled=True,
                    local_enabled=True,
                    redis_enabled=True,
                )

            return redirect('home')

        if action == 'clear_cache':
            cache.clear()
            return redirect('home')

    context = {
        'cache_status': cache.get_status(),
    }
    return render(request, 'main/main.html', context)


class TitleListView(generics.ListAPIView):
    serializer_class = TitleListSerializer
    pagination_class = TitlePagination

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
        params = self.request.query_params
        parts = []

        for key in sorted(params.keys()):
            parts.append(f'{key}={params.get(key)}')

        if not parts:
            return 'titles:list'

        return f'titles:list:{"|".join(parts)}'

    def list(self, request, *args, **kwargs):
        cache_key = self.build_cache_key()

        def factory():
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

        data = cache.get_or_set(
            key=cache_key,
            factory=factory,
            ttl=300,
        )

        return Response(data)


class TopTitlesView(generics.ListAPIView):
    serializer_class = TitleListSerializer
    pagination_class = TitlePagination

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


class TitleSearchView(generics.ListAPIView):
    serializer_class = TitleListSerializer
    pagination_class = TitlePagination

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


class TitleDetailView(APIView):
    def get(self, request, tconst):
        obj = get_object_or_404(Title, tconst=tconst)
        serializer = TitleListSerializer(obj)
        return Response(serializer.data)


class TitleFullDetailView(APIView):
    def get(self, request, tconst):
        obj = get_object_or_404(
            Title.objects
            .select_related('rating')
            .prefetch_related(
                'genres',
                'crew_members__person',
                'principals__person',
            ),
            tconst=tconst,
        )
        serializer = TitleFullSerializer(obj)
        return Response(serializer.data)


class TitleDiscoverView(generics.ListAPIView):
    serializer_class = TitleListSerializer
    pagination_class = TitlePagination

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


class PersonFullDetailView(APIView):
    def get(self, request, nconst):
        obj = get_object_or_404(
            Person.objects.prefetch_related(
                'principal_titles__title__rating',
                'crew_titles__title__rating',
            ),
            nconst=nconst,
        )
        serializer = PersonFullSerializer(obj)
        return Response(serializer.data)


class TopGenresAnalyticsView(APIView):
    def get(self, request):
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

        return Response(data)