# kafka_app/urls.py
from django.urls import path
from .views import start_kafka_consumer

urlpatterns = [
    path('start-consumer/', start_kafka_consumer, name='start_kafka_consumer'),
]
