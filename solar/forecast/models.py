import pandas as pd
import pickle
import xgboost as xgb
from prophet import Prophet

# 1. Load and preprocess data
def load_and_preprocess_data(filepath):
    """
    This function reads an Excel file and selects the relevant columns needed for the prediction. 
    It also ensures the date column is in the correct datetime format.

    Args:
    filepath (str): The path to the Excel file.

    Returns:
    DataFrame: Preprocessed data with the necessary columns.
    """
    try:
        df = pd.read_excel(
            filepath,
            usecols=[
                "ds",  # Date column
                "GHI_A_DATA_Avg",  # Global Horizontal Irradiance
                "POA_A_DATA_2_Avg",  # Plane of Array Irradiance (Sensor 2)
                "AirTC_Avg_Degree Celcius",  # Ambient temperature
                "RH_%",  # Relative Humidity
                "POA_A_DATA_3_Avg",  # Plane of Array Irradiance (Sensor 3)
                "WS_Avg_km/h",  # Wind Speed
                "T110PV_C_Avg_Degree Celcius",  # PV module temperature
            ],
        )
        df["ds"] = pd.to_datetime(df["ds"])  # Convert the date column to datetime
        return df
    except Exception as e:
        raise ValueError(f"Error reading the Excel file: {e}")

# 2. Prepare features using scaler and PCA models
def prepare_features(df, scaler_path, pca_path):
    """
    This function applies scaling and PCA transformation on the input data 
    using pre-trained scaler and PCA models.

    Args:
    df (DataFrame): The dataframe containing the features.
    scaler_path (str): Path to the scaler model (saved as .pkl file).
    pca_path (str): Path to the PCA model (saved as .pkl file).

    Returns:
    Array: Transformed features after applying scaling and PCA.
    """
    try:
        # Load the scaler model
        with open(scaler_path, "rb") as f:
            scaler = pickle.load(f)

        # Load the PCA model
        with open(pca_path, "rb") as f:
            pca = pickle.load(f)

        # Apply scaling and PCA
        features_scaled = scaler.transform(df.iloc[:, 1:])  # Skip the 'ds' column
        features_pca = pca.transform(features_scaled)
        return features_pca
    except Exception as e:
        raise ValueError(f"Error loading scaler or PCA models: {e}")

# 3. Predict power using a trained XGBoost model
def predict_power(features, model_path):
    """
    This function loads a pre-trained XGBoost model from a .bin file 
    and uses it to predict power based on the input features.

    Args:
    features (Array): Scaled and PCA-transformed features.
    model_path (str): Path to the XGBoost model file (.bin).

    Returns:
    Array: Predicted power values.
    """
    try:
        # Load the pre-trained XGBoost model
        model = xgb.XGBRegressor()
        model.load_model(model_path)  # Load the model from the binary file

        # Predict the power using the model
        predicted_power = model.predict(features)
        return predicted_power
    except Exception as e:
        raise ValueError(f"Error loading or using the XGBoost model: {e}")

# 4. Setup and run the Prophet forecasting model
def setup_and_run_prophet(df, params):
    """
    This function initializes and fits a Prophet model with specified parameters.

    Args:
    df (DataFrame): Dataframe with columns 'ds' (date) and 'y' (target variable for forecasting).
    params (dict): Dictionary containing the model parameters for Prophet.

    Returns:
    Prophet: A fitted Prophet model ready for forecasting.
    """
    try:
        # Initialize Prophet with the provided parameters
        m = Prophet(
            yearly_seasonality=params.get("yearly_seasonality", True),
            weekly_seasonality=params.get("weekly_seasonality", True),
            daily_seasonality=params.get("daily_seasonality", True),
            changepoint_prior_scale=params.get("changepoint_prior_scale", 0.5),
            seasonality_prior_scale=params.get("seasonality_prior_scale", 10.0),
            seasonality_mode=params.get("seasonality_mode", "additive"),
            interval_width=params.get("interval_width", 0.95),
        )

        # Add monthly and quarterly seasonality if defined
        if "fourier_monthly" in params:
            m.add_seasonality(
                name="monthly", period=30.5, fourier_order=params["fourier_monthly"]
            )
        if "fourier_quarterly" in params:
            m.add_seasonality(
                name="quarterly", period=91.25, fourier_order=params["fourier_quarterly"]
            )

        # Fit the model to the data
        m.fit(df)
        return m
    except Exception as e:
        raise ValueError(f"Error setting up or running Prophet: {e}")

# 5. Forecast and save results to CSV
def forecast_and_save(model, period, freq, filename):
    """
    This function makes future predictions using a trained Prophet model and saves the 
    forecast results into a CSV file.

    Args:
    model (Prophet): The fitted Prophet model.
    period (int): The number of future periods to forecast.
    freq (str): Frequency of forecast, e.g., 'T' for minute-wise, 'H' for hourly.
    filename (str): Path to the output CSV file where forecast will be saved.

    Returns:
    DataFrame: The forecast dataframe containing the predicted values.
    """
    try:
        # Create future dates for forecasting
        future = model.make_future_dataframe(periods=period, freq=freq)

        # Make predictions
        forecast = model.predict(future)

        # Save the forecasted results
        forecast[["ds", "yhat", "yhat_lower", "yhat_upper"]].to_csv(filename, index=False)
        return forecast
    except Exception as e:
        raise ValueError(f"Error during forecasting or saving results: {e}")
