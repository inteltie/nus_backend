# kafka_app/consumers.py
import json
from channels.generic.websocket import AsyncWebsocketConsumer

class KafkaConsumer(AsyncWebsocketConsumer):
    async def connect(self):
        # Accept the WebSocket connection
        await self.accept()
        print("WebSocket connection established.")

    async def disconnect(self, close_code):
        print("WebSocket disconnected.")

    async def receive(self, text_data):
        print("Message received on WebSocket: ", text_data)

    async def send_kafka_message(self, event):
        # Send Kafka message to WebSocket client
        await self.send(text_data=json.dumps({
            'message': event['message']
        }))
