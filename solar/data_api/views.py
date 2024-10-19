# data_api/views.py
from django.http import JsonResponse
from datetime import datetime, timedelta
import pandas as pd
import os, json
import pytz

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
minute_start_time = pd.to_datetime("18:30").time()  # 6:30 PM
minute_end_time = pd.to_datetime("07:30").time()    # 7:30 AM

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


def get_data(df, feature_column, duration, time_range):
    """Helper function to get data based on feature type, duration, and range."""
    now = datetime.now(IST)  # Get the current time in IST
    
    # Determine start and end based on duration and range
    if duration == 'minute':
        current_start = now.replace(minute=0, second=0, microsecond=0)  # Start of the current hour
        current_end = current_start + timedelta(hours=1)  # End of the current hour
    elif duration == 'hourly':
        current_start = now.replace(hour=0, minute=0, second=0, microsecond=0)  # Start of the current day
        current_end = current_start + timedelta(days=1)  # End of the current day
    else:
        return {'error': 'Invalid duration specified'}

    # Define the start and end times for previous, current, and next ranges
    if time_range == 'current':
        start = current_start
        end = current_end
    elif time_range == 'previous':
        start = current_start - (timedelta(hours=1) if duration == 'minute' else timedelta(days=1))
        end = current_start
    elif time_range == 'next':
        start = current_end
        end = current_end + (timedelta(hours=1) if duration == 'minute' else timedelta(days=1))
    else:
        return {'error': 'Invalid range specified'}

    # Check if the 'ds' column is already timezone-aware
    if df['ds'].dt.tz is None:
        # Localize to IST if not already timezone-aware
        df['ds'] = df['ds'].dt.tz_localize('Asia/Kolkata', ambiguous='NaT', nonexistent='NaT')
    else:
        # Convert to IST if already timezone-aware
        df['ds'] = df['ds'].dt.tz_convert('Asia/Kolkata')
    
    # Filter the data
    data = df[(df['ds'] >= start) & (df['ds'] < end)][['ds', feature_column]]

    data[feature_column] = data[feature_column].apply(lambda x: max(x, 0))

    # Combine the data points based on the range and duration
    result = {
        f'{time_range}_{duration}': data.to_dict(orient='records')
    }
    return result


def generalized_data_api(request):
    feature_type = request.GET.get('feature_type')
    duration = request.GET.get('duration')
    time_range = request.GET.get('range')

    # Define the mapping of feature types to their column names
    feature_mapping = {
        'active_power': 'INVERTER1.1_Active Power_Kw',
        'dc_power': 'INVERTER1.1_DC Power_Kw',
        'todays_gen': 'INVERTER1.1_Todays Gen_Kwh'
    }

    # Select the correct data frame based on duration
    if duration == 'minute':
        df = minute_df
    elif duration == 'hourly':
        df = hourly_df
    else:
        return JsonResponse({'error': 'Invalid duration parameter'}, status=400)

    # Get the corresponding column name for the feature
    feature_column = feature_mapping.get(feature_type)
    if not feature_column:
        return JsonResponse({'error': 'Invalid feature_type parameter'}, status=400)

    # Fetch the data based on the duration and time range
    data = get_data(df, feature_column, duration, time_range)

    return JsonResponse(data, safe=False)

def get_current_minute_data(df, feature):
    """Fetch the current value of the feature from the latest timestamp in IST."""
    # Get current time in IST
    now = datetime.now(IST)
    current_time_ist = now.strftime('%H:%M')
    
    # Debug: Print current time being used
    print(f"Current IST Time for filtering: {current_time_ist}")

    # Check if 'ds' is already timezone-aware
    if df['ds'].dt.tz is None:
        # Localize 'ds' to IST if not timezone-aware
        df['ds'] = df['ds'].dt.tz_localize(IST)
    
    # Filter data matching the current minute
    current_data = df[df['ds'].dt.strftime('%H:%M') == current_time_ist].tail(1)

    # Debug: Check filtered data
    print(f"Filtered Data for {current_time_ist}: {current_data}")

    if not current_data.empty:
        # Extract and return the timestamp and feature value
        timestamp = current_data.iloc[0]['ds'].strftime('%Y-%m-%d %H:%M:%S')
        value = current_data.iloc[0][feature]
        return {'timestamp': timestamp, feature: value}
    else:
        print("No data available for the current minute")
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
