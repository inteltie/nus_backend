# kafka_app/tasks.py

import json
from confluent_kafka import Consumer, KafkaException
from asgiref.sync import async_to_sync
from channels.layers import get_channel_layer
from .consumers import AlertManager

def process_message(data):
    """Send the processed Kafka message to the WebSocket group."""
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

def process_weather_message(data):
    """Send the processed Kafka message to the WebSocket group."""
    print(f"Processing message: {data}")
    # Send the processed message to the WebSocket group
    channel_layer = get_channel_layer()
    async_to_sync(channel_layer.group_send)(
        'weather_group',  # Ensure this matches the group name in the WebSocket consumer
        {
            'type': 'send_weather_message',  # This should match the method in the WebSocket consumer
            'message': data
        }
    )

def run_kafka_consumer():
    """Kafka Consumer for processing inverter data."""
    print("Starting the Kafka consumer task...")

    # Kafka consumer configuration
    consumer_config = {
        'bootstrap.servers': 'b-2.mskclusternus1.8z6j8x.c2.kafka.ap-northeast-2.amazonaws.com:9092,b-1.mskclusternus1.8z6j8x.c2.kafka.ap-northeast-2.amazonaws.com:9092',
        'group.id': 'my-consumer-group-2',
        'auto.offset.reset': 'latest',
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
                # No message received
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
            else:
                # Successfully received a message
                data = json.loads(msg.value().decode('utf-8'))
                print(f"Received message: {data}")

                # Check for out-of-range values
                # out_of_range = AlertManager.check_out_of_range(data)
                # if out_of_range:
                #     print("Data received for out-of-range check:", data)
                #     print("Out-of-range analytics identified:", out_of_range)
                #     # Log to CSV and send WebSocket alert if values are out of range
                #     AlertManager.log_to_csv(data, out_of_range)
                #     AlertManager.send_websocket_alert(out_of_range, data)

                # # Process the message to send to WebSocket group
                # process_message(data)

    except KeyboardInterrupt:
        print("Consumer stopped by user")

    finally:
        consumer.close()


# Weather Data Consumer Task
def run_weather_consumer():
    """Kafka Consumer for processing weather data."""
    print("Starting the Weather Kafka consumer task...")

    consumer_config = {
        'bootstrap.servers': 'b-2.mskclusternus1.8z6j8x.c2.kafka.ap-northeast-2.amazonaws.com:9092,b-1.mskclusternus1.8z6j8x.c2.kafka.ap-northeast-2.amazonaws.com:9092',
        'group.id': 'weather-consumer-group',
        'auto.offset.reset': 'latest',
        'security.protocol': 'PLAINTEXT',
        'max.poll.interval.ms': 900000
    }

    consumer = Consumer(consumer_config)
    topic = 'weather-topic-1'
    consumer.subscribe([topic])

    print(f'Subscribed to Kafka topic: {topic}')

    try:
        while True:
            # Polling messages from Kafka
            msg = consumer.poll(1.0)
            if msg is None:
                # No message received
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
            else:
                # Successfully received a message
                data = json.loads(msg.value().decode('utf-8'))
                print(f"Received weather message: {data}")

                # Check for out-of-range values for weather
                # out_of_range = AlertManager.check_out_of_range(data)
                # if out_of_range:
                #     print("Data received for out-of-range check:", data)
                #     print("Out-of-range analytics identified:", out_of_range)

                #     # Log to CSV and send WebSocket alert if values are out of range
                #     AlertManager.log_to_csv(data, out_of_range)
                #     AlertManager.send_websocket_alert(out_of_range, data)

                # # Process the message to send to WebSocket group
                # process_weather_message(data)

    except KeyboardInterrupt:
        print("Weather consumer stopped by user")

    finally:
        consumer.close()