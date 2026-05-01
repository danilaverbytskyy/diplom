from django.urls import path
from drf_spectacular.views import SpectacularAPIView, SpectacularSwaggerView, SpectacularRedocView

from main.views import (
    home_page,
    PersonFullDetailView,
    TitleDetailView,
    TitleDiscoverView,
    TitleFullDetailView,
    TitleListView,
    TitleSearchView,
    TopGenresAnalyticsView,
    TopTitlesView,
)

urlpatterns = [
    path('', home_page, name='home'),
    path('api/schema/', SpectacularAPIView.as_view(), name='schema'),
    path(
        'api/docs/',
        SpectacularSwaggerView.as_view(url_name='schema'),
        name='swagger-ui',
    ),
    path(
        'api/redoc/',
        SpectacularRedocView.as_view(url_name='schema'),
        name='redoc',
    ),

    path('api/titles/', TitleListView.as_view(), name='title-list'),
    path('api/titles/top/', TopTitlesView.as_view(), name='top-titles'),
    path('api/titles/search/', TitleSearchView.as_view(), name='title-search'),
    path('api/titles/discover/', TitleDiscoverView.as_view(), name='title-discover'),
    path('api/titles/<int:id>/', TitleDetailView.as_view(), name='title-detail'),
    path('api/titles/<int:id>/full/', TitleFullDetailView.as_view(), name='title-full-detail'),

    path('api/persons/<int:id>/', PersonFullDetailView.as_view(), name='person-full-detail'),

    path('api/analytics/top-genres/', TopGenresAnalyticsView.as_view(), name='top-genres-analytics'),
]