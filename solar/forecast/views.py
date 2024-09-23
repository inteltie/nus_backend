from django.http import FileResponse, Http404
from django.utils.http import quote

import os
import pandas as pd
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

from .models import load_and_preprocess_data, prepare_features, predict_power, setup_and_run_prophet, forecast_and_save

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
        # Step 1: Load and preprocess the data
        df = load_and_preprocess_data(file_path)

        # Step 2: Prepare features for prediction using scaler and PCA models
        scaler_path = os.path.join(settings.BASE_DIR, 'forecast', 'models', 'scaler_model.pkl')
        pca_path = os.path.join(settings.BASE_DIR, 'forecast', 'models', 'pca_model.pkl')
        features = prepare_features(df, scaler_path, pca_path)

        if features is None:
            return JsonResponse({'error': 'Failed to prepare features. Check model files.'}, status=500)

        # Step 3: Predict power using a trained XGBoost model
        model_path = os.path.join(settings.BASE_DIR, 'forecast', 'models', 'xgb_model_hyper.bin')
        df['predicted_power'] = predict_power(features, model_path)

        # Step 4: Setup and run the Prophet forecasting model
        params = {
            "yearly_seasonality": True,
            "weekly_seasonality": True,
            "daily_seasonality": True,
            "changepoint_prior_scale": 0.5,
            "seasonality_prior_scale": 10.0,
            "seasonality_mode": "additive",
            "interval_width": 0.95,
            "fourier_monthly": 5,
            "fourier_quarterly": 5,
        }
        df_prophet = df[["ds", "predicted_power"]].rename(columns={"ds": "ds", "predicted_power": "y"})
        prophet_model = setup_and_run_prophet(df_prophet, params)

        # Step 5: Make minute-wise and hourly forecasts
        forecast_minute_file = os.path.join(DATA_FOLDER, 'minute_wise_forecast.csv')
        forecast_hourly_file = os.path.join(DATA_FOLDER, 'hourly_forecast.csv')

        forecast_and_save(prophet_model, 60, 'T', forecast_minute_file)
        forecast_and_save(prophet_model, 24, 'H', forecast_hourly_file)

        # Step 6: Create a zip file containing the forecast files
        zip_file_path = os.path.join(DATA_FOLDER, 'forecast_results.zip')
        with zipfile.ZipFile(zip_file_path, 'w') as zip_file:
            zip_file.write(forecast_minute_file, os.path.basename(forecast_minute_file))
            zip_file.write(forecast_hourly_file, os.path.basename(forecast_hourly_file))

        # Send a response with the zip file path
        response_data = {
            'forecast_zip_file': zip_file_path,
            'message': 'Forecasting completed and saved successfully.'
        }
        return JsonResponse(response_data, status=200)

    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)


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

