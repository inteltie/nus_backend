from django.http import JsonResponse
from .tasks import run_kafka_consumer, run_weather_consumer
import csv
import os
from django.conf import settings

CSV_LOG_FILE = os.path.join(settings.BASE_DIR, 'kafka_app', 'alert_logs.csv')

def start_kafka_consumer(request):
    run_kafka_consumer()
    return JsonResponse({"status": "Kafka consumer started"})

def start_weather_consumer(request):
    run_weather_consumer()
    return JsonResponse({"status": "Weather consumer started"})

def get_alert_logs(request):
    data = []
    with open(CSV_LOG_FILE, 'r') as file:
        reader = csv.reader(file)
        for row in reader:
            data.append({
                "log_timestamp": row[0],
                "data_timestamp": row[1],
                "out_of_range_values": row[2:]
            })
    return JsonResponse(data, safe=False)
