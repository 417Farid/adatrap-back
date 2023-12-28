from django.urls import path, include
#from rest_framework import routers
from .views import prueba

urlpatterns = [
    path('prueba/', prueba)
]   