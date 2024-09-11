# kafka_app/tasks.py
from celery import shared_task
from confluent_kafka import Consumer, KafkaException

@shared_task
def run_kafka_consumer():
    # Debug statement to confirm task start
    print("Starting the Kafka consumer task...")

    # Kafka consumer configuration
    consumer_config = {
        'bootstrap.servers': 'b-2.mskclusternus1.8z6j8x.c2.kafka.ap-northeast-2.amazonaws.com:9092,b-1.mskclusternus1.8z6j8x.c2.kafka.ap-northeast-2.amazonaws.com:9092',
        'group.id': 'my-consumer-group',
        'auto.offset.reset': 'latest',
        'security.protocol': 'PLAINTEXT'
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
    # Debug statement in message processing
    print(f"Processing message: {data}")
