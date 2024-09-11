# kafka_app/urls.py
from django.urls import path
from .views import *

urlpatterns = [
    path('start-consumer/', start_kafka_consumer, name='start_kafka_consumer'),
    path('start-consumer-2/', start_weather_consumer, name='start_weather_consumer'),
]
