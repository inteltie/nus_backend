# kafka_app/tasks.py
# from celery import shared_task
from confluent_kafka import Consumer, KafkaException
from channels.layers import get_channel_layer
from asgiref.sync import async_to_sync

def run_kafka_consumer():
    # Debug statement to confirm task start
    print("Starting the Kafka consumer task...")

    # Kafka consumer configuration
    consumer_config = {
        'bootstrap.servers': 'b-2.mskclusternus1.8z6j8x.c2.kafka.ap-northeast-2.amazonaws.com:9092,b-1.mskclusternus1.8z6j8x.c2.kafka.ap-northeast-2.amazonaws.com:9092',
        'group.id': 'my-consumer-group-2',
        'auto.offset.reset': 'earliest',
        'security.protocol': 'PLAINTEXT',
        'max.poll.interval.ms': 900000
    }

    consumer = Consumer(consumer_config)
    topic = 'inverter-topic-1'
    consumer.subscribe([topic])

    print(f'Subscribed to Kafka topic: {topic}')

    try:
        while True:
            # Polling messages from Kafka
            msg = consumer.poll(1.0)
            if msg is None:
                # Debug message when no message is polled
                print("No message received from Kafka.")
                continue
            if msg.error():
                # Debug message when an error occurs in polling
                print(f"Consumer error: {msg.error()}")
            else:
                # Successfully received a message
                data = msg.value().decode('utf-8')
                print(f"Received message: {data}")
                process_message(data)

    except KeyboardInterrupt:
        print("Consumer stopped by user")

    finally:
        consumer.close()


def process_message(data):
    print(f"Processing message: {data}")
    # Send the processed message to the WebSocket group
    channel_layer = get_channel_layer()
    async_to_sync(channel_layer.group_send)(
        'kafka_group',  # Ensure this matches the group name in the WebSocket consumer
        {
            'type': 'send_kafka_message',  # This should match the method in the WebSocket consumer
            'message': data
        }
    )

# New Kafka Consumer Task for Weather Data
def run_weather_consumer():
    # Kafka consumer configuration
    consumer_config = {
        'bootstrap.servers': 'b-2.mskclusternus1.8z6j8x.c2.kafka.ap-northeast-2.amazonaws.com:9092,b-1.mskclusternus1.8z6j8x.c2.kafka.ap-northeast-2.amazonaws.com:9092',
        'group.id': 'weather-consumer-group',
        'auto.offset.reset': 'earliest',
        'security.protocol': 'PLAINTEXT',
        'max.poll.interval.ms': 900000
    }

    consumer = Consumer(consumer_config)
    topic = 'weather-topic-1'  # Update if necessary
    consumer.subscribe([topic])

    print(f'Subscribed to Kafka topic: {topic}')

    try:
        while True:
            # Polling messages from Kafka
            msg = consumer.poll(1.0)
            if msg is None:
                print("No message received from Kafka.")
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
            else:
                # Successfully received a message
                data = msg.value().decode('utf-8')
                print(f"Received message: {data}")
                process_weather_message(data)

    except KeyboardInterrupt:
        print("Consumer stopped by user")

    finally:
        consumer.close()

# Processing function to handle received weather messages
def process_weather_message(data):
    # Debug statement in message processing
    print(f"Processing weather message: {data}")
    channel_layer = get_channel_layer()
    async_to_sync(channel_layer.group_send)(
        "weather_group",
        {
            "type": "send_weather_message",
            "message": data
        }
    )
