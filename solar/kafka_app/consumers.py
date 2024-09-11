# kafka_app/consumers.py
import json
from channels.generic.websocket import AsyncWebsocketConsumer

class KafkaConsumer(AsyncWebsocketConsumer):
    async def connect(self):
        # Join a group for Kafka messages
        await self.channel_layer.group_add(
            'kafka_group',
            self.channel_name
        )
        await self.accept()
        print("WebSocket connection established.")

    async def disconnect(self, close_code):
        # Leave the group when disconnected
        await self.channel_layer.group_discard(
            'kafka_group',
            self.channel_name
        )
        print("WebSocket disconnected.")

    async def receive(self, text_data):
        print(f"Message received on WebSocket: {text_data}")
        # Echo the received message back to the client (for testing)
        await self.send(text_data=json.dumps({"message": f"Echo: {text_data}"}))

    async def send_kafka_message(self, event):
        # Send Kafka message to WebSocket client
        await self.send(text_data=json.dumps({
            'message': event['message']
        }))

# WeatherConsumer with group integration
class WeatherConsumer(AsyncWebsocketConsumer):
    async def connect(self):
        # Join a group for weather messages
        await self.channel_layer.group_add(
            'weather_group',  # Group for weather-related messages
            self.channel_name
        )
        await self.accept()
        print("Weather WebSocket connection established.")

    async def disconnect(self, close_code):
        # Leave the weather group when disconnected
        await self.channel_layer.group_discard(
            'weather_group',
            self.channel_name
        )
        print("Weather WebSocket disconnected.")

    async def receive(self, text_data):
        print(f"Message received on Weather WebSocket: {text_data}")
        # Echo the received message back to the client (for testing)
        await self.send(text_data=json.dumps({"message": f"Echo: {text_data}"}))

    async def send_weather_message(self, event):
        # Send Weather message to WebSocket client
        await self.send(text_data=json.dumps({
            'message': event['message']
        }))
