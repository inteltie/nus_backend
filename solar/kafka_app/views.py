from django.http import JsonResponse
from .tasks import run_kafka_consumer, run_weather_consumer
import csv
import os
from django.conf import settings
from django.views.decorators.csrf import csrf_exempt

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_LOG_FILE = os.path.join(BASE_DIR, 'kafka_app', 'alert_logs.csv')

def start_kafka_consumer(request):
    run_kafka_consumer()
    return JsonResponse({"status": "Kafka consumer started"})

def start_weather_consumer(request):
    run_weather_consumer()
    return JsonResponse({"status": "Weather consumer started"})

@csrf_exempt
def get_alert_logs(request):
    data = []
    with open(CSV_LOG_FILE, 'r') as file:
        reader = csv.reader(file)
        for row in reader:
            log_entry = {
                "log_timestamp": row[0],
                "data_timestamp": row[1],
                "title": row[2],
                "description": row[3],
                "out_of_range_variables": [
                    var for var in row[4:]  # This assumes remaining columns are out-of-range variables
                ]
            }
            data.append(log_entry)
    return JsonResponse(data, safe=False)

@csrf_exempt
def delete_alert(request, alert_id):
    # Read current logs
    data = []
    with open(CSV_LOG_FILE, 'r') as file:
        reader = csv.reader(file)
        data = list(reader)

    # Remove the specified alert if it exists
    if 0 <= alert_id < len(data):
        deleted_alert = data.pop(alert_id)
        with open(CSV_LOG_FILE, 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerows(data)
        return JsonResponse({"status": "Deleted alert", "deleted_alert": deleted_alert})
    else:
        return JsonResponse({"status": "Alert ID not found"}, status=404)

@csrf_exempt
def delete_all_alerts(request):
    # Clear the CSV file
    open(CSV_LOG_FILE, 'w').close()  # This will truncate the file
    return JsonResponse({"status": "All alerts deleted"})
