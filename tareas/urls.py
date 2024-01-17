from django.urls import path, include
#from rest_framework import routers
from.views import mesFrecuencia
from.views import horaFrecuencia
from.views import diaFrecuencia
from.views import vacasFrecuencia
from.views import rutaFrecuencia
from.views import rutasVelocidad
from.views import horaVelocidad
from.views import cincoRutVelocidad
from.views import mesVelocidad
from.views import diaVelocidad

urlpatterns = [
    # Frecuencia
    path('frecuencia-mes', mesFrecuencia),
    path('frecuencia-hora', horaFrecuencia),
    path('frecuencia-dia', diaFrecuencia),
    path('frecuencia-vacas', vacasFrecuencia),
    path('frecuencia-rutas', rutaFrecuencia),
    # Velocidad
    path('velocidad-rutas', rutasVelocidad),
    path('velocidad-horas', horaVelocidad),
    path('velocidad-cincoRutas', cincoRutVelocidad),
    path('velocidad-mes', mesVelocidad),
    path('velocidad-dia', diaVelocidad)
]   