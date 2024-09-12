# data_api/views.py
from django.http import JsonResponse
from datetime import datetime, timedelta
import pandas as pd
import os
import pytz

IST = pytz.timezone('Asia/Kolkata')

# File paths for the CSV files
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FILE_PATH = os.path.join(BASE_DIR, 'data_api/data/inverter_min.csv')
# Load the hourly inverter data CSV file
HOURLY_FILE_PATH = 'data_api/data/inverter_hourly.csv'

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


# Read the CSV data once to improve performance
hourly_df = pd.read_csv(HOURLY_FILE_PATH)
hourly_df['ds'] = pd.to_datetime(hourly_df['ds'])

def get_daily_data(df, feature_column):
    """Helper function to get the current, previous, and next day data."""
    # Get the current day time range
    today = datetime.now().date()
    current_day_start = datetime.combine(today, datetime.min.time())
    current_day_end = current_day_start + timedelta(days=1)

    # Get previous and next day time ranges
    prev_day_start = current_day_start - timedelta(days=1)
    prev_day_end = current_day_start
    next_day_start = current_day_end
    next_day_end = current_day_end + timedelta(days=1)

    # Filter data for each day range
    current_data = df[(df['ds'] >= current_day_start) & (df['ds'] < current_day_end)][['ds', feature_column]]
    prev_data = df[(df['ds'] >= prev_day_start) & (df['ds'] < prev_day_end)][['ds', feature_column]]
    next_data = df[(df['ds'] >= next_day_start) & (df['ds'] < next_day_end)][['ds', feature_column]]

    # Combine the data
    result = {
        'current_day': current_data.to_dict(orient='records'),
        'prev_day': prev_data.to_dict(orient='records'),
        'next_day': next_data.to_dict(orient='records')
    }
    return result

def active_power_hourly_api(request):
    """API for hourly INVERTER1.1_Active Power_Kw"""
    data = get_daily_data(hourly_df, 'INVERTER1.1_Active Power_Kw')
    return JsonResponse(data)

def dc_power_hourly_api(request):
    """API for hourly INVERTER1.1_DC Power_Kw"""
    data = get_daily_data(hourly_df, 'INVERTER1.1_DC Power_Kw')
    return JsonResponse(data)

def todays_gen_hourly_api(request):
    """API for hourly INVERTER1.1_Todays Gen_Kwh"""
    data = get_daily_data(hourly_df, 'INVERTER1.1_Todays Gen_Kwh')
    return JsonResponse(data)


minute_df = pd.read_csv(FILE_PATH)

# Ensure the 'ds' column is treated as datetime
minute_df['ds'] = pd.to_datetime(minute_df['ds'])

def get_current_minute_data(df, feature):
    """Fetch the current value of the feature from the latest timestamp in IST."""
    current_time_ist = datetime.now(IST).strftime('%H:%M')
    current_data = df[df['ds'].dt.tz_localize('UTC').dt.tz_convert('Asia/Kolkata').dt.strftime('%H:%M') == current_time_ist].tail(1)
    
    if not current_data.empty:
        timestamp = current_data.iloc[0]['ds'].tz_localize('UTC').tz_convert('Asia/Kolkata').strftime('%Y-%m-%d %H:%M:%S')
        value = current_data.iloc[0][feature]
        return {'timestamp': timestamp, feature: value}
    else:
        return {'error': 'No data available for the current minute'}

# API for Current Active Power
def current_active_power_api(request):
    data = get_current_minute_data(minute_df, 'INVERTER1.1_Active Power_Kw')
    return JsonResponse(data)

# API for Current DC Power
def current_dc_power_api(request):
    data = get_current_minute_data(minute_df, 'INVERTER1.1_DC Power_Kw')
    return JsonResponse(data)

# API for Current Today's Generation
def current_todays_gen_api(request):
    data = get_current_minute_data(minute_df, 'INVERTER1.1_Todays Gen_Kwh')
    return JsonResponse(data)