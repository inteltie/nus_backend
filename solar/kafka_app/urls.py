from django.urls import path
from .views import start_kafka_consumer, start_weather_consumer, get_alert_logs

urlpatterns = [
    path('start-consumer/', start_kafka_consumer, name='start_kafka_consumer'),
    path('start-weather-consumer/', start_weather_consumer, name='start_weather_consumer'),
    path('logs/', get_alert_logs, name='get_alert_logs'),
]
