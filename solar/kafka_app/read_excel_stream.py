# kafka_app/read_excel_stream.py
import pandas as pd
from datetime import datetime
from zoneinfo import ZoneInfo
import asyncio

async def stream_csv_data_per_minute(
    csv_path: str,
    timestamp_column: str = "ds",
    delay_seconds: int = 60,
    timezone: str = "Asia/Kolkata"
):
    """
    Asynchronously streams each row from the CSV where ds column contains a timestamp.
    Yields rows from the current time forward (Asia/Kolkata time), one every `delay_seconds`.
    """
    tz = ZoneInfo(timezone)

    df = pd.read_csv(csv_path)
    df[timestamp_column] = pd.to_datetime(df[timestamp_column], dayfirst=True, errors='coerce')
    df = df.dropna(subset=[timestamp_column])

    df[timestamp_column] = df[timestamp_column].dt.tz_localize(tz)
    df = df.sort_values(by=timestamp_column).reset_index(drop=True)

    now = datetime.now(tz).replace(second=0, microsecond=0)

    start_index = df[df[timestamp_column] <= now].shape[0] - 1
    if start_index < 0:
        print(f"No matching timestamp <= current time ({now}).")
        return

    for idx in range(start_index, len(df)):
        row = df.loc[idx].copy()
        ds_str = row[timestamp_column].strftime("%Y-%m-%d %H:%M:%S")

        row_dict = row.to_dict()
        row_dict[timestamp_column] = ds_str
        row_dict["adjusted_ds"] = ds_str

        yield row_dict
        await asyncio.sleep(delay_seconds)