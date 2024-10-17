from django.http import FileResponse, Http404
from django.utils.http import quote

from sklearn.preprocessing import StandardScaler
import tensorflow as tf
from tensorflow.keras.models import load_model
import joblib
import sys

import os
import pandas as pd
import numpy as np
import zipfile
from django.conf import settings
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.core.files.storage import default_storage
from django.core.files.base import ContentFile
from django.http import HttpResponseBadRequest
from django.shortcuts import get_object_or_404
from rest_framework.decorators import api_view
from rest_framework.parsers import MultiPartParser, FormParser
from rest_framework.response import Response

from .models import (
    load_new_data,
    clean_data,
    feature_engineering,
    define_features,
    generate_time_features,
    forecast_future
)

from rest_framework import status
from datetime import timedelta

# DATA_FOLDER = os.path.join(settings.BASE_DIR, 'data')

# Path to the 'data' folder in the forecast app
DATA_FOLDER = os.path.join(settings.BASE_DIR, 'forecast', 'data')

if not os.path.exists(DATA_FOLDER):
    os.makedirs(DATA_FOLDER)  # Create the folder if it doesn't exist

@csrf_exempt
@api_view(['POST'])
def upload_and_predict(request):
    """
    API endpoint to upload an Excel file and make predictions/forecast.
    The uploaded Excel file is saved to the 'data' folder within 'forecast', 
    and the predictions are saved in a zip file containing the forecast CSVs.
    """
    if 'file' not in request.FILES:
        return HttpResponseBadRequest('Missing file in request.')

    excel_file = request.FILES['file']

    # Save the uploaded Excel file to the 'data' folder
    file_path = os.path.join(DATA_FOLDER, excel_file.name)
    with default_storage.open(file_path, 'wb+') as destination:
        for chunk in excel_file.chunks():
            destination.write(chunk)

    try:
    # Paths to the saved model and scalers
        model_file = os.path.join(settings.BASE_DIR, 'forecast', 'models', 'final_model.keras')  # or 'best_model.keras' if that's your final model
        weather_scaler_file = os.path.join(settings.BASE_DIR, 'forecast', 'models', 'weather_scaler.pkl')
        inverter_scaler_file = os.path.join(settings.BASE_DIR, 'forecast', 'models', 'inverter_scaler.pkl')

        # Load the new data
        df_new = load_new_data(file_path)

        # Preprocess the new data (same as before)
        df_new = clean_data(df_new)
        df_new = feature_engineering(df_new)
        weather_features, time_features, inverter_features = define_features(df_new)

        # Check if inverter features are present; if so, drop them
        df_new = df_new.drop(
            columns=[col for col in inverter_features if col in df_new.columns]
        )

        # Set the same window size as used during training
        WINDOW_SIZE = 60  # Adjust if different

        # Create sequences for prediction
        X_new = []
        timestamps = []
        data = df_new.copy().reset_index(drop=True)
        total_features = weather_features + time_features

        for i in range(len(data) - WINDOW_SIZE + 1):
            X_seq = data.loc[i : i + WINDOW_SIZE - 1, total_features].values
            timestamp = data.loc[i + WINDOW_SIZE - 1, "ds"]
            if X_seq.shape == (WINDOW_SIZE, len(total_features)):
                X_new.append(X_seq)
                timestamps.append(timestamp)
            else:
                print(f"Skipping sequence at index {i} due to inconsistent shapes.")

        if len(X_new) == 0:
            print("No valid sequences were created. Please check your data.")
            sys.exit(1)

        X_new = np.array(X_new)
        timestamps = np.array(timestamps)

        # Load the scalers
        if not os.path.exists(weather_scaler_file) or not os.path.exists(
            inverter_scaler_file
        ):
            print(
                "Scaler files not found. Please ensure 'weather_scaler.pkl' and 'inverter_scaler.pkl' are in the current directory."
            )
            sys.exit(1)

        weather_scaler = joblib.load(weather_scaler_file)
        inverter_scaler = joblib.load(inverter_scaler_file)

        # Scale the features
        num_samples_new, window_size, num_features = X_new.shape
        X_new_reshaped = X_new.reshape(-1, num_features)
        X_new_scaled = weather_scaler.transform(X_new_reshaped).reshape(
            num_samples_new, window_size, num_features
        )

        # Load the model
        if not os.path.exists(model_file):
            print(
                f"Model file '{model_file}' not found. Please ensure the model file is in the current directory."
            )
            sys.exit(1)

        model = load_model(model_file)

        # Make predictions on the new data
        y_pred_scaled = model.predict(X_new_scaled)

        # Inverse transform the predictions
        y_pred = inverter_scaler.inverse_transform(y_pred_scaled)

        # Save the predictions along with timestamps
        df_predictions = pd.DataFrame(y_pred, columns=inverter_features)
        df_predictions["ds"] = timestamps
        cols = ["ds"] + inverter_features
        df_predictions = df_predictions[cols]

        # Save the original minute-wise predictions to Excel
        predictions_file = os.path.join(DATA_FOLDER, "predictions_minute_wise.xlsx")
        df_predictions.to_excel(predictions_file, index=False)
        print(f"\nPredictions saved to '{predictions_file}'")

        # Resample predictions to hourly frequency
        df_predictions_hourly = df_predictions.set_index('ds').resample('H').mean().reset_index()

        # Save hourly predictions to Excel
        predictions_hourly_file = os.path.join(DATA_FOLDER, "predictions_hour_wise.xlsx")
        df_predictions_hourly.to_excel(predictions_hourly_file, index=False)
        print(f"Hourly predictions saved to '{predictions_hourly_file}'")

        # Forecasting future values
        print("\nForecasting future values...")

        # Prepare initial sequence for forecasting
        initial_sequence = X_new[-1]  # Last sequence from the new data
        last_timestamp = timestamps[-1]

        # Forecast next 100 minutes (minute-wise)
        forecast_steps_minute = 100
        forecasted_values_minute, forecasted_timestamps_minute = forecast_future(
            model,
            initial_sequence,
            weather_scaler,
            inverter_scaler,
            WINDOW_SIZE,
            forecast_steps_minute,
            last_timestamp,
            weather_features,
            freq="T",  # 'T' for minute frequency
        )

        # Create DataFrame for minute-wise forecasts
        df_forecast_minute = pd.DataFrame(
            forecasted_values_minute, columns=inverter_features
        )
        df_forecast_minute["ds"] = forecasted_timestamps_minute
        cols = ["ds"] + inverter_features
        df_forecast_minute = df_forecast_minute[cols]
        df_concat_minute = pd.concat([df_predictions, df_forecast_minute], ignore_index=True).sort_values(by="ds")

        # Save minute-wise forecasts to Excel
        forecast_minute_file = os.path.join(DATA_FOLDER, "forecast_minute_wise.xlsx")
        df_concat_minute.to_excel(forecast_minute_file, index=False)
        print(f"Minute-wise forecasts saved to '{forecast_minute_file}'")

        # Forecast next 24 hours (hour-wise)
        forecast_steps_hour = 24
        forecasted_values_hour, forecasted_timestamps_hour = forecast_future(
            model,
            initial_sequence,
            weather_scaler,
            inverter_scaler,
            WINDOW_SIZE,
            forecast_steps_hour,
            last_timestamp,
            weather_features,
            freq="H",  # 'H' for hourly frequency
        )

        # Create DataFrame for hour-wise forecasts
        df_forecast_hour = pd.DataFrame(forecasted_values_hour, columns=inverter_features)
        df_forecast_hour["ds"] = forecasted_timestamps_hour
        cols = ["ds"] + inverter_features
        df_forecast_hour = df_forecast_hour[cols]
        df_concat_hourly = pd.concat([df_predictions_hourly, df_forecast_hour], ignore_index=True).sort_values(by="ds")

        # Save hour-wise forecasts to Excel
        forecast_hour_file = os.path.join(DATA_FOLDER, "forecast_hour_wise.xlsx")
        df_concat_hourly.to_excel(forecast_hour_file, index=False)
        print(f"Hour-wise forecasts saved to '{forecast_hour_file}'")

        print("\nForecasting completed.")

        # Step 6: Create a zip file containing the forecast files
        zip_file_path = os.path.join(DATA_FOLDER, 'forecast_results.zip')
        with zipfile.ZipFile(zip_file_path, 'w') as zip_file:
            zip_file.write(forecast_minute_file, os.path.basename(forecast_minute_file))
            zip_file.write(forecast_hour_file, os.path.basename(forecast_hour_file))

        # Send a response with the zip file path
        response_data = {
            'forecast_zip_file': zip_file_path,
            'message': 'Forecasting completed and saved successfully.'
        }
        return JsonResponse(response_data, status=200)

    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)


@csrf_exempt
@api_view(['GET'])
def download_forecast(request):
    """
    API endpoint to download the forecast zip file.
    """
    zip_file_path = os.path.join(DATA_FOLDER, 'forecast_results.zip')

    if not os.path.exists(zip_file_path):
        raise Http404("Forecast zip file does not exist.")

    # Send the zip file as a response without opening it manually
    return FileResponse(open(zip_file_path, 'rb'), as_attachment=True, filename='forecast_results.zip')


@csrf_exempt
@api_view(['GET'])
def download_sample(request):
    """
    API endpoint to download the sameple zip file.
    """
    zip_file_path = os.path.join(DATA_FOLDER, 'sample_format.zip')

    if not os.path.exists(zip_file_path):
        raise Http404("Sample zip file does not exist.")

    # Send the zip file as a response without opening it manually
    return FileResponse(open(zip_file_path, 'rb'), as_attachment=True, filename='sample_format.zip')


@csrf_exempt
@api_view(['GET'])
def download_comparison(request):
    """
    API endpoint to download the comparison zip file.
    """
    zip_file_path = os.path.join(DATA_FOLDER, 'power_output_comparison.zip')

    if not os.path.exists(zip_file_path):
        raise Http404("comparison zip file does not exist.")

    # Send the zip file as a response without opening it manually
    return FileResponse(open(zip_file_path, 'rb'), as_attachment=True, filename='download_comparison.zip')


@csrf_exempt
@api_view(['POST'])
def compare_power_output(request):
    # Check if the actual power output file is uploaded
    if 'file' not in request.FILES:
        return HttpResponseBadRequest('Missing file in request.')

    excel_file = request.FILES['file']

    # Save the uploaded Excel file to the 'data' folder
    file_path = os.path.join(DATA_FOLDER, excel_file.name)
    with default_storage.open(file_path, 'wb+') as destination:
        for chunk in excel_file.chunks():
            destination.write(chunk)

    try:
        df_actual_min = load_new_data(file_path)

        df_actual_hourly = df_actual_min.set_index('ds').resample('H').mean().reset_index()

        # Path to minute-wise weather forecast file
        min_forecast_path = os.path.join(DATA_FOLDER, 'forecast_minute_wise.xlsx')
        hour_forecast_path = os.path.join(DATA_FOLDER, 'forecast_hour_wise.xlsx')
        
        if not os.path.exists(min_forecast_path):
            return Response({"error": "Forecast file not found. Please upload weather data first."}, 
                            status=status.HTTP_400_BAD_REQUEST)

        # Read the actual power output and minute-wise forecast files
        df_forecast_min = load_new_data(min_forecast_path)
        df_forecast_hour = load_new_data(hour_forecast_path)

        # Ensure the 'ds' column is in datetime format for both DataFrames
        df_actual_min['ds'] = pd.to_datetime(df_actual_min['ds'], errors='coerce')
        df_actual_hourly['ds'] = pd.to_datetime(df_actual_hourly['ds'], errors='coerce')
        df_forecast_min['ds'] = pd.to_datetime(df_forecast_min['ds'], errors='coerce')
        df_forecast_hour['ds'] = pd.to_datetime(df_forecast_hour['ds'], errors='coerce')

        # Drop rows with NaT in 'ds' after conversion
        df_actual_min.dropna(subset=['ds'], inplace=True)
        df_actual_hourly.dropna(subset=['ds'], inplace=True)
        df_forecast_min.dropna(subset=['ds'], inplace=True)
        df_forecast_hour.dropna(subset=['ds'], inplace=True)

        # Combine the actual and predicted power data (retain only ds, predicted_power, actual_power)
        # For MINUTE wise
        predicted_power_min = df_forecast_min[['ds', 'INVERTER1.1_Active Power_Kw']].rename(columns={'INVERTER1.1_Active Power_Kw': 'predicted_power'})
        combined_min_data = df_actual_min.merge(predicted_power_min, on='ds', how='right').rename(
            columns={'INVERTER1.1_Active Power_Kw': 'actual_power'})

        # Step 1: Keep only 'ds', 'predicted_power', and 'actual_power'
        combined_min_data = combined_min_data[['ds', 'predicted_power', 'actual_power']]

        # Step 2: Fill missing 'actual_power' with NaN (if not already handled by merge)
        combined_min_data['actual_power'] = combined_min_data['actual_power'].fillna(value=np.nan)

        # Step 1: Save the entire combined data (all rows) to a CSV file
        comparison_csv_min = os.path.join(DATA_FOLDER, 'power_min_comparison.csv')
        combined_min_data.to_csv(comparison_csv_min, index=False)

        # For HOURLY
        predicted_power_hour = df_forecast_hour[['ds', 'INVERTER1.1_Active Power_Kw']].rename(columns={'INVERTER1.1_Active Power_Kw': 'predicted_power'})
        combined_full_data_hour = df_actual_hourly.merge(predicted_power_hour, on='ds', how='right').rename(
            columns={'INVERTER1.1_Active Power_Kw': 'actual_power'})

        # Step 1: Keep only 'ds', 'predicted_power', and 'actual_power'
        combined_full_data_hour = combined_full_data_hour[['ds', 'predicted_power', 'actual_power']]

        # Step 2: Fill missing 'actual_power' with NaN (if not already handled by merge)
        combined_full_data_hour['actual_power'] = combined_full_data_hour['actual_power'].fillna(value=np.nan)

        # Step 1: Save the entire combined data (all rows) to a CSV file
        comparison_csv_hour = os.path.join(DATA_FOLDER, 'power_hour_comparison.csv')
        combined_full_data_hour.to_csv(comparison_csv_hour, index=False)


        zip_file_path = os.path.join(DATA_FOLDER, 'power_output_comparison.zip')
        with zipfile.ZipFile(zip_file_path, 'w') as zip_file:
            zip_file.write(comparison_csv_min, os.path.basename(comparison_csv_min))
            zip_file.write(comparison_csv_hour, os.path.basename(comparison_csv_hour))

        first_date = combined_full_data_hour['ds'].min()
        last_date = combined_full_data_hour['ds'].max()

        # Prepare the response data
        response_data = {
            'first_date': first_date,
            'last_date': last_date,
            'data': {}
        }

        df_resampled_monthly = combined_min_data.set_index('ds').resample('M').mean().reset_index()

        response_data['data'] = []
        for _, row in df_resampled_monthly.iterrows():
            month_key = row['ds'].strftime('%Y-%m')

            actual_power = max(row['actual_power'], 0) if pd.notna(row['actual_power']) else 0
            predicted_power = max(row['predicted_power'], 0) if pd.notna(row['predicted_power']) else 0

            response_data['data'].append({
                'timestamp': row['ds'].strftime('%Y-%m'),
                'actual_power': actual_power,
                'predicted_power': predicted_power,
                'month': month_key
            })

        return Response(response_data, status=status.HTTP_200_OK)
    except Exception as e:
        return Response({"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


@csrf_exempt
@api_view(['GET'])
def get_power_comparison(request):
    # Get the graph_type parameter from the query parameters
    graph_type = request.GET.get('graph_type')

    if graph_type not in ['minute', 'hourly', 'daily', 'all_time']:
        return Response({"error": "Invalid graph_type parameter. Must be 'minute', 'hourly', 'daily', or 'all_time'."}, 
                        status=status.HTTP_400_BAD_REQUEST)

    try:
        # Determine the file to read based on graph_type
        if graph_type == 'minute':
            comparison_csv_path = os.path.join(DATA_FOLDER, 'power_min_comparison.csv')
        elif graph_type == 'hourly':
            comparison_csv_path = os.path.join(DATA_FOLDER, 'power_hour_comparison.csv')
        else:
            comparison_csv_path = os.path.join(DATA_FOLDER, 'power_min_comparison.csv')

        # Check if the file exists
        if not os.path.exists(comparison_csv_path):
            return Response({"error": "Comparison file not found."}, status=status.HTTP_404_NOT_FOUND)

        # Load the CSV file into a DataFrame with the first row as header
        df_comparison = pd.read_csv(comparison_csv_path, header=0)

        # Convert 'ds' column to datetime
        df_comparison['ds'] = pd.to_datetime(df_comparison['ds'], errors='coerce')

        # Check if there are any valid dates
        if df_comparison['ds'].isnull().all():
            return Response({"error": "No valid datetime entries found."}, status=status.HTTP_404_NOT_FOUND)

        # Prepare the response data
        response_data = {
            'first_date': df_comparison['ds'].min(),
            'last_date': df_comparison['ds'].max(),
            'data': {}
        }

        # Resample data for daily and all_time
        df_resampled_daily = df_comparison.set_index('ds').resample('D').mean().reset_index()
        df_resampled_monthly = df_comparison.set_index('ds').resample('M').mean().reset_index()

        if graph_type == 'hourly':
            # Group by day
            for _, row in df_comparison.iterrows():
                day_key = row['ds'].strftime(f'%Y-%m-%d')
                if day_key not in response_data['data']:
                    response_data['data'][day_key] = []
                
                actual_power = max(row['actual_power'], 0) if pd.notna(row['actual_power']) else 0
                predicted_power = max(row['predicted_power'], 0) if pd.notna(row['predicted_power']) else 0

                response_data['data'][day_key].append({
                    'timestamp': row['ds'].strftime(f'%Y-%m-%d %H:%M'),
                    'actual_power': actual_power,
                    'predicted_power': predicted_power
                })

        elif graph_type == 'minute':
            # Combine by hour
            for _, row in df_comparison.iterrows():
                hour_key = row['ds'].strftime(f'%Y-%m-%d %H:%M')
                if hour_key not in response_data['data']:
                    response_data['data'][hour_key] = []

                actual_power = max(row['actual_power'], 0) if pd.notna(row['actual_power']) else 0
                predicted_power = max(row['predicted_power'], 0) if pd.notna(row['predicted_power']) else 0

                response_data['data'][hour_key].append({
                    'timestamp': row['ds'].strftime(f'%Y-%m-%d %H:%M:%S'),
                    'actual_power': actual_power,
                    'predicted_power': predicted_power
                })

        elif graph_type == 'daily':
            # Group daily data by month
            for _, row in df_resampled_daily.iterrows():
                day_key = row['ds'].strftime(f'%Y-%m-%d')
                month_key = row['ds'].strftime('%Y-%m')

                if month_key not in response_data['data']:
                    response_data['data'][month_key] = []

                actual_power = max(row['actual_power'], 0) if pd.notna(row['actual_power']) else 0
                predicted_power = max(row['predicted_power'], 0) if pd.notna(row['predicted_power']) else 0

                response_data['data'][month_key].append({
                    'actual_power': actual_power,
                    'predicted_power': predicted_power,
                    'timestamp': day_key
                })

        elif graph_type == 'all_time':
            # Group all data month-wise
            response_data['data'] = []
            for _, row in df_resampled_monthly.iterrows():
                month_key = row['ds'].strftime('%Y-%m')

                actual_power = max(row['actual_power'], 0) if pd.notna(row['actual_power']) else 0
                predicted_power = max(row['predicted_power'], 0) if pd.notna(row['predicted_power']) else 0

                response_data['data'].append({
                    'timestamp': row['ds'].strftime('%Y-%m'),
                    'actual_power': actual_power,
                    'predicted_power': predicted_power,
                    'month': month_key
                })

        return Response(response_data, status=status.HTTP_200_OK)

    except Exception as e:
        return Response({"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
