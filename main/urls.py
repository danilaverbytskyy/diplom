from django.urls import path

from .api.views import HealthView
from .views import main

urlpatterns = [
    path('api/health/', HealthView.as_view(), name='health'),
    path('', main, name='main'),
]