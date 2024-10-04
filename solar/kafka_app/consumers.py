import json, os, csv
from django.conf import settings
from asgiref.sync import async_to_sync
from datetime import datetime
from channels.generic.websocket import AsyncWebsocketConsumer
from channels.layers import get_channel_layer

# Define file paths
ANALYTICS_FILE_PATH = os.path.join(settings.BASE_DIR, 'data_api', 'data', 'analytics.json')
CSV_LOG_FILE = os.path.join(settings.BASE_DIR, 'kafka_app', 'alert_logs.csv')

# Load analytics range data
def load_analytics():
    with open(ANALYTICS_FILE_PATH, 'r') as file:
        return json.load(file)

analytics_data = load_analytics()


# Helper class for Alerts
class AlertManager:
    @staticmethod
    def check_out_of_range(data):
        """Check if incoming data is out of range for any analytics."""
        out_of_range_values = {}
        for key, thresholds in analytics_data.items():
            if key in data:
                value = data[key]
                if value < thresholds['min'] or value > thresholds['max']:
                    out_of_range_values[key] = value
        return out_of_range_values

    @staticmethod
    def log_to_csv(data, out_of_range_values):
        """Log the out-of-range values in a CSV."""
        with open(CSV_LOG_FILE, mode='a', newline='') as file:
            writer = csv.writer(file)
            row = [datetime.now(), data['timestamp']] + [f"{key}: {val}" for key, val in out_of_range_values.items()]
            writer.writerow(row)

    @staticmethod
    def send_websocket_alert(out_of_range_values, data):
        """Send alert via WebSocket."""
        channel_layer = get_channel_layer()
        message = {
            "type": "alert",
            "timestamp": data['timestamp'],
            "out_of_range_values": out_of_range_values
        }
        async_to_sync(channel_layer.group_send)(
            "alerts_group", {"type": "send_alert", "message": message}
        )


# Consumers
class KafkaConsumer(AsyncWebsocketConsumer):
    async def connect(self):
        await self.channel_layer.group_add('kafka_group', self.channel_name)
        await self.accept()

    async def disconnect(self, close_code):
        await self.channel_layer.group_discard('kafka_group', self.channel_name)

    async def receive(self, text_data):
        await self.send(text_data=json.dumps({"message": f"Echo: {text_data}"}))

    async def send_kafka_message(self, event):
        await self.send(text_data=json.dumps({'message': event['message']}))


class WeatherConsumer(AsyncWebsocketConsumer):
    async def connect(self):
        await self.channel_layer.group_add('weather_group', self.channel_name)
        await self.accept()

    async def disconnect(self, close_code):
        await self.channel_layer.group_discard('weather_group', self.channel_name)

    async def receive(self, text_data):
        await self.send(text_data=json.dumps({"message": f"Echo: {text_data}"}))

    async def send_weather_message(self, event):
        await self.send(text_data=json.dumps({'message': event['message']}))


class AlertConsumer(AsyncWebsocketConsumer):
    async def connect(self):
        await self.channel_layer.group_add("alerts_group", self.channel_name)
        await self.accept()

    async def disconnect(self, close_code):
        await self.channel_layer.group_discard("alerts_group", self.channel_name)

    async def send_alert(self, event):
        await self.send(text_data=json.dumps(event['message']))
