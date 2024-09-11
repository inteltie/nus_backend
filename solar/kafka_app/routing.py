# kafka_app/routing.py
from django.urls import re_path
from . import consumers

websocket_urlpatterns = [
    re_path(r'ws/kafka/', consumers.KafkaConsumer.as_asgi()),
]
