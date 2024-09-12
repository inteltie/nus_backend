# data_api/views.py
from django.http import JsonResponse
from datetime import datetime, timedelta
import pandas as pd
import os

# File paths for the CSV files
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FILE_PATH = os.path.join(BASE_DIR, 'data_api/data/inverter_min.csv')
#weather_file_path = os.path.join(BASE_DIR, 'data_api/data/Weather_min.csv')

# Read the CSV data once to improve performance
df = pd.read_csv(FILE_PATH)
df['ds'] = pd.to_datetime(df['ds'])

def get_hourly_data(df, feature_column):
    """Helper function to get the current, previous, and next hour data."""
    # Get the current hour time range
    now = datetime.now()
    current_hour_start = now.replace(minute=0, second=0, microsecond=0)
    current_hour_end = current_hour_start + timedelta(hours=1)

    # Get previous and next hour time ranges
    prev_hour_start = current_hour_start - timedelta(hours=1)
    prev_hour_end = current_hour_start
    next_hour_start = current_hour_end
    next_hour_end = current_hour_end + timedelta(hours=1)

    # Filter data for each hour range
    current_data = df[(df['ds'] >= current_hour_start) & (df['ds'] < current_hour_end)][['ds', feature_column]]
    prev_data = df[(df['ds'] >= prev_hour_start) & (df['ds'] < prev_hour_end)][['ds', feature_column]]
    next_data = df[(df['ds'] >= next_hour_start) & (df['ds'] < next_hour_end)][['ds', feature_column]]

    # Combine the data
    result = {
        'current_hour': current_data.to_dict(orient='records'),
        'prev_hour': prev_data.to_dict(orient='records'),
        'next_hour': next_data.to_dict(orient='records')
    }
    return result

def active_power_api(request):
    """API for INVERTER1.1_Active Power_Kw"""
    data = get_hourly_data(df, 'INVERTER1.1_Active Power_Kw')
    return JsonResponse(data)

def dc_power_api(request):
    """API for INVERTER1.1_DC Power_Kw"""
    data = get_hourly_data(df, 'INVERTER1.1_DC Power_Kw')
    return JsonResponse(data)

def todays_gen_api(request):
    """API for INVERTER1.1_Todays Gen_Kwh"""
    data = get_hourly_data(df, 'INVERTER1.1_Todays Gen_Kwh')
    return JsonResponse(data)