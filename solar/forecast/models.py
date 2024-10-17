import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
import tensorflow as tf
from tensorflow.keras.models import load_model
import joblib
import sys
import os

# Suppress TensorFlow warnings for cleaner output
import logging

logging.getLogger("tensorflow").setLevel(logging.ERROR)


def load_new_data(excel_file):
    """
    Loads new data from an Excel file.
    """
    try:
        df = pd.read_excel(excel_file)
        print("First 5 rows of the new dataset:")
        print(df.head())
        return df
    except Exception as e:
        print(f"Error loading Excel file: {e}")
        sys.exit(1)


def clean_data(df):
    """
    Cleans the dataframe by parsing datetime, sorting, and handling missing values.
    """
    try:
        if "ds" not in df.columns:
            raise ValueError("The dataframe must contain a 'ds' column for datetime.")

        df["ds"] = pd.to_datetime(df["ds"], errors="coerce")
        df.dropna(subset=["ds"], inplace=True)
        df = df.sort_values("ds")
        df.fillna(method="ffill", inplace=True)

        if df.isnull().sum().sum() == 0:
            print("\nNo missing values remain after cleaning.")
        else:
            print("\nMissing values still present after cleaning:")
            print(df.isnull().sum())

        return df
    except Exception as e:
        print(f"Error during data cleaning: {e}")
        sys.exit(1)


def feature_engineering(df):
    """
    Adds time-based and cyclical features to the dataframe.
    """
    try:
        df["minute"] = df["ds"].dt.minute
        df["hour"] = df["ds"].dt.hour
        df["day"] = df["ds"].dt.day
        df["day_of_week"] = df["ds"].dt.dayofweek
        df["month"] = df["ds"].dt.month
        df["is_weekend"] = df["day_of_week"].apply(lambda x: 1 if x >= 5 else 0)

        def add_cyclical_features(df, column, max_val):
            df[f"{column}_sin"] = np.sin(2 * np.pi * df[column] / max_val)
            df[f"{column}_cos"] = np.cos(2 * np.pi * df[column] / max_val)
            return df

        time_columns = {"minute": 60, "hour": 24, "day_of_week": 7, "month": 12}
        for col, max_val in time_columns.items():
            df = add_cyclical_features(df, col, max_val)

        df.drop(columns=["minute", "hour", "day_of_week", "month"], inplace=True)

        print("\nDataframe after feature engineering:")
        print(df.head())

        return df
    except Exception as e:
        print(f"Error during feature engineering: {e}")
        sys.exit(1)


def define_features(df):
    """
    Defines weather and inverter feature lists.
    """
    weather_features = [
        "GHI_A_DATA_Avg", "POA_A_DATA_2_Avg", "AirTC_Avg_Degree Celcius",
        "RH_%", "POA_A_DATA_3_Avg", "WS_Avg_km/h", "T110PV_C_Avg_Degree Celcius"
    ]
    time_features = [
        "is_weekend", "minute_sin", "minute_cos", "hour_sin", "hour_cos",
        "day_of_week_sin", "day_of_week_cos", "month_sin", "month_cos"
    ]
    inverter_features = [
        "INVERTER1.1_Active Power_Kw", "INVERTER1.1_Todays Gen_Kwh", "INVERTER1.1_DC Power_Kw"
    ]

    missing_weather = [feat for feat in weather_features if feat not in df.columns]
    missing_time = [feat for feat in time_features if feat not in df.columns]

    if missing_weather:
        raise ValueError(f"Missing weather features: {missing_weather}")
    if missing_time:
        raise ValueError(f"Missing time features: {missing_time}")

    return weather_features, time_features, inverter_features


def generate_time_features(timestamp):
    """
    Generates time-based and cyclical features for a given timestamp.
    """
    minute = timestamp.minute
    hour = timestamp.hour
    day_of_week = timestamp.dayofweek
    month = timestamp.month
    is_weekend = 1 if day_of_week >= 5 else 0

    minute_sin = np.sin(2 * np.pi * minute / 60)
    minute_cos = np.cos(2 * np.pi * minute / 60)
    hour_sin = np.sin(2 * np.pi * hour / 24)
    hour_cos = np.cos(2 * np.pi * hour / 24)
    day_of_week_sin = np.sin(2 * np.pi * day_of_week / 7)
    day_of_week_cos = np.cos(2 * np.pi * day_of_week / 7)
    month_sin = np.sin(2 * np.pi * month / 12)
    month_cos = np.cos(2 * np.pi * month / 12)

    time_features = [
        is_weekend, minute_sin, minute_cos, hour_sin, hour_cos,
        day_of_week_sin, day_of_week_cos, month_sin, month_cos
    ]
    return np.array(time_features)


def forecast_future(
    model, initial_sequence, weather_scaler, inverter_scaler, window_size,
    forecast_steps, last_timestamp, weather_features, freq="T"
):
    """
    Forecasts future inverter features based on the initial weather sequence.
    """
    try:
        forecasted = []
        current_sequence = initial_sequence.copy()

        forecasted_timestamps = pd.date_range(
            start=last_timestamp + pd.Timedelta(1, unit=freq), periods=forecast_steps, freq=freq
        )

        for step in range(forecast_steps):
            scaled_seq = weather_scaler.transform(current_sequence).reshape(1, window_size, -1)
            pred_scaled = model.predict(scaled_seq)
            pred = inverter_scaler.inverse_transform(pred_scaled)

            forecasted.append(pred.flatten())

            new_timestamp = forecasted_timestamps[step]
            new_time_features = generate_time_features(new_timestamp)
            last_weather = current_sequence[-1][:len(weather_features)]
            new_weather_data = last_weather
            new_input = np.concatenate([new_weather_data, new_time_features])
            current_sequence = np.vstack([current_sequence[1:], new_input])

        return np.array(forecasted), forecasted_timestamps

    except Exception as e:
        print(f"Error during future forecasting: {e}")
        sys.exit(1)

