# kafka_app/views.py
# kafka_app/views.py
from django.http import JsonResponse
from .tasks import run_kafka_consumer, run_weather_consumer

def start_kafka_consumer(request):
    run_kafka_consumer()  # Starts the existing Kafka consumer
    return JsonResponse({"status": "Kafka consumer started"})

def start_weather_consumer(request):
    run_weather_consumer()  # Starts the new Weather consumer
    return JsonResponse({"status": "Weather consumer started"})
