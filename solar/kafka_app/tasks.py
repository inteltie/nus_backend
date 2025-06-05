# kafka_app/tasks.py

import json
from confluent_kafka import Consumer, KafkaException
from asgiref.sync import async_to_sync
from channels.layers import get_channel_layer
from .consumers import AlertManager
from .read_excel_stream import stream_csv_data_per_minute
import os

# File paths for the CSV files
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FILE_INV_MINUTE = os.path.join(BASE_DIR, "kafka_app/excel_data/inv_min_2.csv")
FILE_WEATHER_MINUTE = os.path.join(BASE_DIR, "kafka_app/excel_data/Weather_min.csv")


async def process_message(data):
    print(f"Processing message: {data}")
    channel_layer = get_channel_layer()
    await channel_layer.group_send(
        "kafka_group", {"type": "send_kafka_message", "message": data}
    )


async def process_weather_message(data):
    """Send the processed Kafka message to the WebSocket group."""
    print(f"Processing message: {data}")
    # Send the processed message to the WebSocket group
    channel_layer = get_channel_layer()
    await channel_layer.group_send(
        "weather_group",  # Ensure this matches the group name in the WebSocket consumer
        {
            "type": "send_weather_message",  # This should match the method in the WebSocket consumer
            "message": data,
        },
    )


async def run_kafka_consumer():
    """Kafka Consumer for processing inverter data."""
    # print("Starting the Kafka consumer task...")

    # # Kafka consumer configuration
    # consumer_config = {
    #     'bootstrap.servers': 'b-2.mskclusternus1.8z6j8x.c2.kafka.ap-northeast-2.amazonaws.com:9092,b-1.mskclusternus1.8z6j8x.c2.kafka.ap-northeast-2.amazonaws.com:9092',
    #     'group.id': 'my-consumer-group-2',
    #     'auto.offset.reset': 'latest',
    #     'security.protocol': 'PLAINTEXT',
    #     'max.poll.interval.ms': 900000
    # }

    # consumer = Consumer(consumer_config)
    # topic = 'inverter-topic-1'
    # consumer.subscribe([topic])

    # print(f'Subscribed to Kafka topic: {topic}')

    try:
    #     while True:
    #         # Polling messages from Kafka
    #         msg = consumer.poll(1.0)
    #         if msg is None:
    #             # No message received
    #             continue
    #         if msg.error():
    #             print(f"Consumer error: {msg.error()}")
    #         else:
    #             # Successfully received a message
    #             data = json.loads(msg.value().decode('utf-8'))
    #             print(f"Received message: {data}")
        async for row_data in stream_csv_data_per_minute(FILE_INV_MINUTE, delay_seconds=60):
            data = row_data
            # Check for out-of-range values
            out_of_range = AlertManager.check_out_of_range(data)
            if out_of_range:
                print("Data received for out-of-range check:", data)
                print("Out-of-range analytics identified:", out_of_range)
                # Log to CSV and send WebSocket alert if values are out of range
                AlertManager.log_to_csv(data, out_of_range)
                await AlertManager.send_websocket_alert(out_of_range, data)

            # Process the message to send to WebSocket group
            await process_message(data)

    except KeyboardInterrupt:
        print("Consumer stopped by user")

    # finally:
    #     consumer.close()


# Weather Data Consumer Task
async def run_weather_consumer():
    """Kafka Consumer for processing weather data."""
    # print("Starting the Weather Kafka consumer task...")

    # consumer_config = {
    #     'bootstrap.servers': 'b-2.mskclusternus1.8z6j8x.c2.kafka.ap-northeast-2.amazonaws.com:9092,b-1.mskclusternus1.8z6j8x.c2.kafka.ap-northeast-2.amazonaws.com:9092',
    #     'group.id': 'weather-consumer-group',
    #     'auto.offset.reset': 'latest',
    #     'security.protocol': 'PLAINTEXT',
    #     'max.poll.interval.ms': 900000
    # }

    # consumer = Consumer(consumer_config)
    # topic = 'weather-topic-1'
    # consumer.subscribe([topic])

    # print(f'Subscribed to Kafka topic: {topic}')

    try:
        #     while True:
        #         # Polling messages from Kafka
        #         msg = consumer.poll(1.0)
        #         if msg is None:
        #             # No message received
        #             continue
        #         if msg.error():
        #             print(f"Consumer error: {msg.error()}")
        #         else:
        #             # Successfully received a message
        #             data = json.loads(msg.value().decode('utf-8'))
        #             print(f"Received weather message: {data}")

                # Check for out-of-range values for weather
        async for row_data in stream_csv_data_per_minute(FILE_WEATHER_MINUTE, delay_seconds=60):
            data = row_data
            out_of_range = AlertManager.check_out_of_range(data)
            if out_of_range:
                print("Data received for out-of-range check:", data)
                print("Out-of-range analytics identified:", out_of_range)

                # Log to CSV and send WebSocket alert if values are out of range
                AlertManager.log_to_csv(data, out_of_range)
                await AlertManager.send_websocket_alert(out_of_range, data)

            # Process the message to send to WebSocket group
            await process_weather_message(data)

    except KeyboardInterrupt:
        print("Weather consumer stopped by user")

    # finally:
    #     consumer.close()
