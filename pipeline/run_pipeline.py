import pandas as pd
from dotenv import load_dotenv

from pipeline.ingestion.extract import extract_payload
from pipeline.transformations.transform import transform_data
from pipeline.loaders.s3_loader import upload_dataframe_to_s3
from scripts.api_parameters import build_forecast_params, build_historical_params
from config.config import HISTORY_URL, FORECAST_URL

load_dotenv()

locations_df = pd.read_csv("config/locations.csv")
locations = locations_df.to_dict(orient="records")

def main():
    run_timestamp = pd.Timestamp.now(tz="UTC")
    historical_params = build_historical_params(
         locations_df= locations_df,
         start_date= "2025-08-01",
         end_date= "2026-07-31",
    )
    historical_data = extract_payload(
        HISTORY_URL,
        historical_params
    )

    forecast_params = build_forecast_params(locations_df)
    forcast_data = extract_payload(
        url= FORECAST_URL,
        params= forecast_params,
    )

    historical_hourly, historical_daily = transform_data(
        data= historical_data,
        locations = locations,
        run_timestamp= run_timestamp
    )

    forecast_hourly, forecast_daily = transform_data(
        data= forcast_data,
        locations= locations,
        run_timestamp= run_timestamp
    )

    historical_hourly_uris= upload_dataframe_to_s3(
        historical_hourly,
        "historical_hourly"
    )
    historical_daily_uris= upload_dataframe_to_s3(
        historical_daily,
        "historical_daily"
    )

    forecast_hourly_uris = upload_dataframe_to_s3(
        forecast_hourly,
        "forecast_hourly"
    )
    forecast_daily_uris = upload_dataframe_to_s3(
        forecast_daily,
        "forecast_daily"
    )

    print(f"historical_hourly uri: {len(historical_hourly_uris)}")
    print(f"historical_daily uri: {len(historical_daily_uris)}")
    print(f"forcast_hourly uri: {len(forecast_hourly_uris)}")
    print(f"forcast_daily uri: {len(forecast_daily_uris)}")

if __name__ == "__main__":
    main()

    

