import json, os, csv
from django.conf import settings
from asgiref.sync import async_to_sync
from datetime import datetime
from channels.generic.websocket import AsyncWebsocketConsumer
from channels.layers import get_channel_layer
import uuid

# Define file paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ANALYTICS_FILE_PATH = os.path.join(BASE_DIR, 'data_api', 'data', 'analytics.json')
CSV_LOG_FILE = os.path.join(BASE_DIR, 'kafka_app', 'alert_logs.csv')

# Load analytics range data
def load_analytics():
    with open(ANALYTICS_FILE_PATH, 'r') as file:
        return json.load(file)

analytics_data = load_analytics()

# Helper class for Alerts
class AlertManager:
    @staticmethod
    def check_out_of_range(inverter_data, weather_data):
        out_of_range_analytics = []
        combined_data = {**inverter_data, **weather_data}  # Combine both inverter and weather data
        print(f"Checking out-of-range for combined data: {combined_data}")  # Debugging statement

        for analytic in analytics_data.get('analytics', []):
            if analytic.get("selected"):
                out_of_range_variables = {}
                print(f"Processing analytic: {analytic['title']}")  # Debugging statement
                for variable, thresholds in analytic.get("variables", {}).items():
                    if variable in combined_data:
                        value = combined_data[variable]
                        print(f"Checking variable: {variable}, value: {value}, thresholds: {thresholds}")  # Debugging
                        if value < thresholds["min"] or value > thresholds["max"]:
                            out_of_range_variables[variable] = {
                                'actual_value': value,
                                'expected_range': thresholds
                            }

                if out_of_range_variables:
                    out_of_range_analytics.append({
                        'title': analytic['title'],
                        'description': analytic['description'],
                        'out_of_range_variables': out_of_range_variables
                    })

        if out_of_range_analytics:
            print(f"Out-of-range analytics found: {out_of_range_analytics}\n\n")  # Debugging statement
        else:
            print("No out-of-range analytics found.\n\n")  # Debugging statement

        return out_of_range_analytics


    @staticmethod
    def log_to_csv(data, out_of_range_analytics):
        print(f"Logging data: {out_of_range_analytics}")  # Debugging statement

        # Step 1: Read existing alerts to count and trim if needed
        existing_alerts = []
        if os.path.exists(CSV_LOG_FILE):
            with open(CSV_LOG_FILE, mode='r') as file:
                reader = csv.reader(file)
                existing_alerts = list(reader)

        # Step 2: Trim the list if it exceeds 49, as we'll add one more alert below
        if len(existing_alerts) >= 50:
            existing_alerts = existing_alerts[-49:]  # Keep only the latest 49 entries

        # Step 3: Append new alerts to existing list and save
        with open(CSV_LOG_FILE, mode='w', newline='') as file:
            writer = csv.writer(file)
            for alert in existing_alerts:
                writer.writerow(alert)  # Write back trimmed existing alerts

            # Write new alerts
            for analytic in out_of_range_analytics:
                unique_id = str(uuid.uuid4())  # Generate a unique ID
                row = [
                    unique_id,  # Unique identifier
                    datetime.now(), 
                    data.get('ds'), 
                    analytic['title'], 
                    analytic['description']
                ] + [
                    f"{var}: {details['actual_value']} (expected {details['expected_range']['min']} - {details['expected_range']['max']})" 
                    for var, details in analytic['out_of_range_variables'].items()
                ]
                writer.writerow(row)  # Write new alert

    @staticmethod
    def send_websocket_alert(out_of_range_analytics, data):
        print(f"Sending WebSocket alert for: {out_of_range_analytics}")  # Debugging statement
        if not out_of_range_analytics:
            print("No alerts to send via WebSocket.")  # Debugging statement
            return

        channel_layer = get_channel_layer()
        for analytic in out_of_range_analytics:
            message = {
                "type": "alert",
                "timestamp": data['ds'],
                "title": analytic['title'],
                "description": analytic['description'],
                "out_of_range_variables": analytic['out_of_range_variables']
            }
            print(f"Prepared WebSocket message: {message}\n\n")  # Debugging statement
            async_to_sync(channel_layer.group_send)(
                "alerts_group", {"type": "send_alert", "message": message}
            )


# Consumers for Kafka and Weather Data

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

