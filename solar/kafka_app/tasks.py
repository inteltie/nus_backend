# kafka_app/tasks.py
from celery import shared_task
from confluent_kafka import Consumer, KafkaException

@shared_task
def run_kafka_consumer():
    # Kafka consumer configuration
    consumer_config = {
        'bootstrap.servers': 'b-2.mskclusternus1.8z6j8x.c2.kafka.ap-northeast-2.amazonaws.com:9092,b-1.mskclusternus1.8z6j8x.c2.kafka.ap-northeast-2.amazonaws.com:9092',
        'group.id': 'my-consumer-group',
        'auto.offset.reset': 'earliest',
        'security.protocol': 'PLAINTEXT'  
    }

    consumer = Consumer(consumer_config)
    topic = 'inverter-topic'
    consumer.subscribe([topic])

    print(f'Starting Kafka consumer for topic: {topic}')

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaException._PARTITION_EOF:
                    print(f"End of partition reached {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                data = msg.value().decode('utf-8')
                print(f"Received message: {data}")
                # Process the message, e.g., save to the database
                process_message(data)

    except KeyboardInterrupt:
        print("Consumer stopped by user")

    finally:
        consumer.close()

def process_message(data):
    # Custom logic to handle messages
    print(f"Processing message: {data}")
    # For example, create a database entry using Django models
