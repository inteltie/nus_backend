# data_api/views.py
from django.http import JsonResponse
from rest_framework.response import Response
from datetime import datetime, timedelta
from rest_framework import status
import pandas as pd
import os, json
import pytz
#libraries

from django.views.decorators.csrf import csrf_exempt
from django.shortcuts import render
from django.conf import settings

IST = pytz.timezone('Asia/Kolkata')

# File paths for the CSV files
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FILE_PATH_MINUTE = os.path.join(BASE_DIR, 'data_api/data/inv_min_2.csv')
FILE_PATH_HOURLY = os.path.join(BASE_DIR, 'data_api/data/inverter_hourly.csv')

# Read the CSV data once to improve performance
minute_df = pd.read_csv(FILE_PATH_MINUTE)
minute_df['ds'] = pd.to_datetime(minute_df['ds'])

# Time ranges for replacing values
minute_start_time = pd.to_datetime("19:00").time()  # 6:30 PM
minute_end_time = pd.to_datetime("07:00").time()    # 7:30 AM

# Set actual_power and predicted_power to 0 during the specified time range for MINUTE wise
minute_df['time'] = minute_df['ds'].dt.time
minute_df.loc[
    ((minute_df['time'] >= minute_start_time) | (minute_df['time'] <= minute_end_time)),
    ["INVERTER1.1_Active Power_Kw", "INVERTER1.1_AC Current (A)_Phase 1", "INVERTER1.1_AC Current (A)_Phase 2", "INVERTER1.1_AC Current (A)_Phase 3", "INVERTER1.1_AC Voltage (V)_BR", "INVERTER1.1_AC Voltage (V)_RY", "INVERTER1.1_AC Voltage (V)_YB", "INVERTER1.1_Todays Gen_Kwh", "INVERTER1.1_DC Current", "INVERTER1.1_DC Voltage", "INVERTER1.1_DC Power_Kw", "INVERTER1.1_Reactive power_Kvar", "INVERTER1.1_Inverter_Temp.", "INVERTER1.1_Power Factor"]] = 0
minute_df.drop('time', axis=1, inplace=True)

minute_df[["INVERTER1.1_Active Power_Kw", "INVERTER1.1_AC Current (A)_Phase 1", "INVERTER1.1_AC Current (A)_Phase 2", "INVERTER1.1_AC Current (A)_Phase 3", "INVERTER1.1_AC Voltage (V)_BR", "INVERTER1.1_AC Voltage (V)_RY", "INVERTER1.1_AC Voltage (V)_YB", "INVERTER1.1_Todays Gen_Kwh", "INVERTER1.1_DC Current", "INVERTER1.1_DC Voltage", "INVERTER1.1_DC Power_Kw", "INVERTER1.1_Reactive power_Kvar", "INVERTER1.1_Inverter_Temp.", "INVERTER1.1_Power Factor"]] = minute_df[["INVERTER1.1_Active Power_Kw", "INVERTER1.1_AC Current (A)_Phase 1", "INVERTER1.1_AC Current (A)_Phase 2", "INVERTER1.1_AC Current (A)_Phase 3", "INVERTER1.1_AC Voltage (V)_BR", "INVERTER1.1_AC Voltage (V)_RY", "INVERTER1.1_AC Voltage (V)_YB", "INVERTER1.1_Todays Gen_Kwh", "INVERTER1.1_DC Current", "INVERTER1.1_DC Voltage", "INVERTER1.1_DC Power_Kw", "INVERTER1.1_Reactive power_Kvar", "INVERTER1.1_Inverter_Temp.", "INVERTER1.1_Power Factor"]].clip(lower=0)

hourly_df = pd.read_csv(FILE_PATH_HOURLY)
hourly_df['ds'] = pd.to_datetime(hourly_df['ds'])

# Time ranges for replacing values
hour_start_time = pd.to_datetime("19:00").time()  # 6:00 PM
hour_end_time = pd.to_datetime("07:00").time()    # 8:00 AM

# Set actual_power and predicted_power to 0 during the specified time range for MINUTE wise
hourly_df['time'] = hourly_df['ds'].dt.time
hourly_df.loc[
    ((hourly_df['time'] >= hour_start_time) | (hourly_df['time'] <= hour_end_time)),
    ["INVERTER1.1_Active Power_Kw", "INVERTER1.1_AC Current (A)_Phase 1", "INVERTER1.1_AC Current (A)_Phase 2", "INVERTER1.1_AC Current (A)_Phase 3", "INVERTER1.1_AC Voltage (V)_BR", "INVERTER1.1_AC Voltage (V)_RY", "INVERTER1.1_AC Voltage (V)_YB", "INVERTER1.1_Todays Gen_Kwh", "INVERTER1.1_DC Current", "INVERTER1.1_DC Voltage", "INVERTER1.1_DC Power_Kw", "INVERTER1.1_Reactive power_Kvar", "INVERTER1.1_Inverter_Temp.", "INVERTER1.1_Power Factor"]] = 0
hourly_df.drop('time', axis=1, inplace=True)

hourly_df[["INVERTER1.1_Active Power_Kw", "INVERTER1.1_AC Current (A)_Phase 1", "INVERTER1.1_AC Current (A)_Phase 2", "INVERTER1.1_AC Current (A)_Phase 3", "INVERTER1.1_AC Voltage (V)_BR", "INVERTER1.1_AC Voltage (V)_RY", "INVERTER1.1_AC Voltage (V)_YB", "INVERTER1.1_Todays Gen_Kwh", "INVERTER1.1_DC Current", "INVERTER1.1_DC Voltage", "INVERTER1.1_DC Power_Kw", "INVERTER1.1_Reactive power_Kvar", "INVERTER1.1_Inverter_Temp.", "INVERTER1.1_Power Factor"]] = hourly_df[["INVERTER1.1_Active Power_Kw", "INVERTER1.1_AC Current (A)_Phase 1", "INVERTER1.1_AC Current (A)_Phase 2", "INVERTER1.1_AC Current (A)_Phase 3", "INVERTER1.1_AC Voltage (V)_BR", "INVERTER1.1_AC Voltage (V)_RY", "INVERTER1.1_AC Voltage (V)_YB", "INVERTER1.1_Todays Gen_Kwh", "INVERTER1.1_DC Current", "INVERTER1.1_DC Voltage", "INVERTER1.1_DC Power_Kw", "INVERTER1.1_Reactive power_Kvar", "INVERTER1.1_Inverter_Temp.", "INVERTER1.1_Power Factor"]].clip(lower=0)

@csrf_exempt
def generalized_data_api(request):
    feature_type = request.GET.get('feature_type')
    graph_type = request.GET.get('graph_type')

    # Define the mapping of feature types to their column names
    feature_mapping = {
        'active_power': 'INVERTER1.1_Active Power_Kw',
        'dc_power': 'INVERTER1.1_DC Power_Kw',
        'todays_gen': 'INVERTER1.1_Todays Gen_Kwh'
    }

    if graph_type not in ['minute', 'hourly']:
        return JsonResponse({"error": "Invalid graph_type parameter. Must be 'minute' or 'hourly'."}, 
                             status=status.HTTP_400_BAD_REQUEST)

    try:
        # Determine the file to read based on graph_type
        if graph_type == 'minute':
            df_data = minute_df
        else:
            df_data = hourly_df

        # Prepare the response data
        response_data = {
            'first_date': df_data['ds'].min(),
            'last_date': df_data['ds'].max(),
            'data': {}
        }

        df_data['ds'] = pd.to_datetime(df_data['ds'], errors='coerce')

        # Extract and group the data
        feature_column = feature_mapping[feature_type]

        if graph_type == 'hourly':
            # Group by day
            for _, row in df_data.iterrows():
                day_key = row['ds'].strftime(f'%Y-%m-%d')
                if day_key not in response_data['data']:
                    response_data['data'][day_key] = []
                
                value = max(row[feature_column], 0) if pd.notna(row[feature_column]) else 0

                response_data['data'][day_key].append({
                    'timestamp': row['ds'].strftime(f'%Y-%m-%d %H:%M'),
                    'value': value
                })

        elif graph_type == 'minute':
            # Group by hour
            for _, row in df_data.iterrows():
                hour_key = row['ds'].strftime(f'%Y-%m-%d %H:00')  # Set to the start of the hour
                if hour_key not in response_data['data']:
                    response_data['data'][hour_key] = []  # Initialize as a list if it doesn't exist

                value = max(row[feature_column], 0) if pd.notna(row[feature_column]) else 0

                # Append the current row's data to the list for that hour
                response_data['data'][hour_key].append({
                    'timestamp': row['ds'].strftime(f'%Y-%m-%d %H:%M:%S'),  # Keep the minute in the timestamp
                    'value': value
                })

        return JsonResponse(response_data, status=status.HTTP_200_OK)

    except Exception as e:
        return JsonResponse({"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


# Path to analytics.json file
ANALYTICS_FILE_PATH = os.path.join(settings.BASE_DIR, 'data_api/data/analytics.json')

def load_analytics():
    """Load the analytics.json file and return its content."""
    with open(ANALYTICS_FILE_PATH, 'r') as file:
        return json.load(file)

def save_analytics(data):
    """Save updated analytics data to the analytics.json file."""
    with open(ANALYTICS_FILE_PATH, 'w') as file:
        json.dump(data, file, indent=4)

def settings_page(request):
    """View to display analytics settings and variables."""
    analytics_data = load_analytics()  # Load the analytics.json data
    return JsonResponse(analytics_data, safe=False)

@csrf_exempt
def save_settings(request):
    """View to handle saving user-selected settings."""
    if request.method == 'POST':
        # Load existing analytics data
        analytics_data = load_analytics()

        # Get updated data from the frontend request (JSON payload)
        updated_data = json.loads(request.body)

        # Update analytics.json with new user selections and thresholds
        for updated_analytic in updated_data['analytics']:
            for analytic in analytics_data['analytics']:
                if analytic['title'] == updated_analytic['title']:
                    analytic['selected'] = updated_analytic['selected']
                    for variable, thresholds in updated_analytic['variables'].items():
                        analytic['variables'][variable]['min'] = thresholds['min']
                        analytic['variables'][variable]['max'] = thresholds['max']

        # Save the updated analytics data
        save_analytics(analytics_data)

        return JsonResponse({'message': 'Settings updated successfully.'}, status=200)

    return JsonResponse({'error': 'Invalid request method.'}, status=400)
