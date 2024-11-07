# data_api/views.py
from django.http import JsonResponse
from datetime import datetime, timedelta
from rest_framework import status
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
minute_start_time = pd.to_datetime("19:00").time()  # 6:30 PM
minute_end_time = pd.to_datetime("07:00").time()    # 7:30 AM

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
hour_start_time = pd.to_datetime("20:00").time()  # 6:00 PM
hour_end_time = pd.to_datetime("07:00").time()    # 8:00 AM

# Set actual_power and predicted_power to 0 during the specified time range for MINUTE wise
hourly_df['time'] = hourly_df['ds'].dt.time
hourly_df.loc[
    ((hourly_df['time'] >= hour_start_time) | (hourly_df['time'] <= hour_end_time)),
    ["PR", "n_system", "Capacity_Factor", "Specific_Yield_kWh_kWp", "Energy_Yield_per_Area_kWh_m2", "Degradation_Rate_%_per_minute", "Insulation_Resistance_MOhm"]] = 0
hourly_df.drop('time', axis=1, inplace=True)

hourly_df[["PR", "n_system", "Capacity_Factor", "Specific_Yield_kWh_kWp", "Energy_Yield_per_Area_kWh_m2", "Degradation_Rate_%_per_minute", "Insulation_Resistance_MOhm"]] = hourly_df[["PR", "n_system", "Capacity_Factor", "Specific_Yield_kWh_kWp", "Energy_Yield_per_Area_kWh_m2", "Degradation_Rate_%_per_minute", "Insulation_Resistance_MOhm"]].clip(lower=0)

@csrf_exempt
def derived_data(request):
    feature_type = request.GET.get('feature_type')
    graph_type = request.GET.get('graph_type')

    # Define the mapping of feature types to their column names
    feature_mapping = {
        'pr': 'PR',
        'n_system': 'n_system',
        'capacity_factor': 'Capacity_Factor',
        'specific_yied': 'Specific_Yield_kWh_kWp',
        'energy_yeild': 'Energy_Yield_per_Area_kWh_m2',
        'degradation': 'Degradation_Rate_%_per_minute',
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

                # Add the new logic here to adjust values
                if feature_column in ['PR', 'n_system', 'Capacity_Factor', 'Degradation_Rate_%_per_minute']:
                    # Apply the new logic
                    random_integer = random.randint(85, 90)
                    adjusted_value = min(value * 100, random_integer)
                    value = adjusted_value  # Update the value

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

                # Add the new logic here to adjust values
                if feature_column in ['PR', 'n_system', 'Capacity_Factor', 'Degradation_Rate_%_per_minute']:
                    # Apply the new logic
                    random_integer = random.randint(85, 90)
                    adjusted_value = min(value * 100, random_integer)
                    value = adjusted_value  # Update the value

                # Append the current row's data to the list for that hour
                response_data['data'][hour_key].append({
                    'timestamp': row['ds'].strftime(f'%Y-%m-%d %H:%M:%S'),  # Keep the minute in the timestamp
                    'value': value
                })

        return JsonResponse(response_data, status=status.HTTP_200_OK)

    except Exception as e:
        return JsonResponse({"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)



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

