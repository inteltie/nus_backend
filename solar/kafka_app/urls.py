from django.urls import path
from .views import start_kafka_consumer, start_weather_consumer, get_alert_logs, delete_alert, delete_all_alerts

urlpatterns = [
    path('start-consumer/', start_kafka_consumer, name='start_kafka_consumer'),
    path('start-weather-consumer/', start_weather_consumer, name='start_weather_consumer'),
    path('logs/', get_alert_logs, name='get_alert_logs'),
    path('delete_alert/<uuid:alert_id>/', delete_alert, name='delete_alert'),
    path('delete_all_alerts/', delete_all_alerts, name='delete_all_alerts'),
]