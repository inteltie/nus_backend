# kafka_app/views.py
from django.http import JsonResponse
from .tasks import run_kafka_consumer

def start_kafka_consumer(request):
    """
    A view to start the Kafka consumer task using Celery.
    This triggers the task asynchronously.
    """
    # Trigger the Kafka consumer task asynchronously
    run_kafka_consumer.delay()

    # Return a JSON response indicating the task has been started
    return JsonResponse({'status': 'Kafka consumer started'})
