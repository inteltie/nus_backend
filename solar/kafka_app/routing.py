# kafka_app/routing.py
from django.urls import re_path
from . import consumers
from .consumers import *

websocket_urlpatterns = [
    re_path(r'ws/kafka/', consumers.KafkaConsumer.as_asgi()),  # Existing route for inverter data
    re_path(r'ws/kafka2/', consumers.WeatherConsumer.as_asgi()),  # New route for weather data
    re_path(r'ws/alerts/', consumers.AlertConsumer.as_asgi()),
]






