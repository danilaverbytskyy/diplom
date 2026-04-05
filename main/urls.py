from django.urls import path

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

    path('api/titles/', TitleListView.as_view(), name='title-list'),
    path('api/titles/top/', TopTitlesView.as_view(), name='top-titles'),
    path('api/titles/search/', TitleSearchView.as_view(), name='title-search'),
    path('api/titles/discover/', TitleDiscoverView.as_view(), name='title-discover'),
    path('api/titles/<str:tconst>/', TitleDetailView.as_view(), name='title-detail'),
    path('api/titles/<str:tconst>/full/', TitleFullDetailView.as_view(), name='title-full-detail'),

    path('api/persons/<str:nconst>/full/', PersonFullDetailView.as_view(), name='person-full-detail'),

    path('api/analytics/top-genres/', TopGenresAnalyticsView.as_view(), name='top-genres-analytics'),
]