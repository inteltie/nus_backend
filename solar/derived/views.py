# data_api/views.py
from django.http import JsonResponse
from datetime import datetime, timedelta
import pandas as pd
import os, json, random
import pytz

from django.views.decorators.csrf import csrf_exempt
from rest_framework.decorators import api_view
from django.shortcuts import render
from django.conf import settings

IST = pytz.timezone('Asia/Kolkata')

# File paths for the CSV files
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FILE_PATH_MINUTE = os.path.join(BASE_DIR, 'derived/data/derived_min.csv')
FILE_PATH_HOURLY = os.path.join(BASE_DIR, 'derived/data/der_hourly.csv')

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
    ["PR", "n_system", "Capacity_Factor", "Specific_Yield_kWh_kWp", "Energy_Yield_per_Area_kWh_m2", "Degradation_Rate_%_per_minute", "Insulation_Resistance_MOhm"]] = 0
minute_df.drop('time', axis=1, inplace=True)

minute_df[["PR", "n_system", "Capacity_Factor", "Specific_Yield_kWh_kWp", "Energy_Yield_per_Area_kWh_m2", "Degradation_Rate_%_per_minute", "Insulation_Resistance_MOhm"]] = minute_df[["PR", "n_system", "Capacity_Factor", "Specific_Yield_kWh_kWp", "Energy_Yield_per_Area_kWh_m2", "Degradation_Rate_%_per_minute", "Insulation_Resistance_MOhm"]].clip(lower=0)

# HOURLY
hourly_df = pd.read_csv(FILE_PATH_HOURLY)
hourly_df['ds'] = pd.to_datetime(hourly_df['ds'])

# Time ranges for replacing values
hour_start_time = pd.to_datetime("19:00").time()  # 6:00 PM
hour_end_time = pd.to_datetime("08:00").time()    # 8:00 AM

# Set actual_power and predicted_power to 0 during the specified time range for MINUTE wise
hourly_df['time'] = hourly_df['ds'].dt.time
hourly_df.loc[
    ((hourly_df['time'] >= hour_start_time) | (hourly_df['time'] <= hour_end_time)),
    ["PR", "n_system", "Capacity_Factor", "Specific_Yield_kWh_kWp", "Energy_Yield_per_Area_kWh_m2", "Degradation_Rate_%_per_minute", "Insulation_Resistance_MOhm"]] = 0
hourly_df.drop('time', axis=1, inplace=True)

hourly_df[["PR", "n_system", "Capacity_Factor", "Specific_Yield_kWh_kWp", "Energy_Yield_per_Area_kWh_m2", "Degradation_Rate_%_per_minute", "Insulation_Resistance_MOhm"]] = hourly_df[["PR", "n_system", "Capacity_Factor", "Specific_Yield_kWh_kWp", "Energy_Yield_per_Area_kWh_m2", "Degradation_Rate_%_per_minute", "Insulation_Resistance_MOhm"]].clip(lower=0)


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


@csrf_exempt
def derived_data(request):
    feature_type = request.GET.get('feature_type')
    duration = request.GET.get('duration')
    time_range = request.GET.get('range')

    # Define the mapping of feature types to their column names
    feature_mapping = {
        'pr': 'PR',
        'n_system': 'n_system',
        'capacity_factor': 'Capacity_Factor',
        'specific_yied': 'Specific_Yield_kWh_kWp',
        'energy_yeild': 'Energy_Yield_per_Area_kWh_m2',
        'degradation': 'Degradation_Rate_%_per_minute',
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

    # Modify the values as per your requirements
    if feature_column in ['PR', 'n_system', 'Capacity_Factor', 'Degradation_Rate_%_per_minute']:
        for record in data[f'{time_range}_{duration}']:
            value = record[feature_column]
            # Multiply by 100 and cap it at 90
            random_integer = random.randint(85, 90)

            adjusted_value = min(value * 100, random_integer)
            record[feature_column] = adjusted_value

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

