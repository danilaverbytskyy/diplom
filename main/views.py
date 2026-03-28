from django.shortcuts import render
from rest_framework.views import APIView
from rest_framework.response import Response


def main(request):
    return render(request, 'main/main.html')