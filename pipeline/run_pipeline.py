import pandas as pd
from dotenv import load_dotenv

from pipeline.ingestion.extract import extract_payload
from pipeline.transformations.transform import transform_data
from pipeline.loaders.s3_loader import upload_dataframe_to_s3
from pipeline.loaders.snowflake_loader import (
    create_snowflake_connection,
    replace_snowflake_partition,
    replace_forecast_snapshot,
)
from scripts.api_parameters import build_forecast_params, build_historical_params
from config.config import HISTORY_URL, FORECAST_URL

load_dotenv()

locations_df = pd.read_csv("config/locations.csv")
locations = locations_df.to_dict(orient="records")

def get_partition_dates(data: pd.DataFrame) -> list[str]:
    return (
        pd.to_datetime(data["time"], errors="raise")
        .dt.strftime("%Y-%m-%d")
        .drop_duplicates()
        .tolist()
    )

def main():
    run_timestamp = pd.Timestamp.now(tz="UTC")
    historical_params = build_historical_params(
         locations_df= locations_df,
         start_date= "2025-08-01",
         end_date=  "2025-08-01",
    )
    historical_data = extract_payload(
        HISTORY_URL,
        historical_params
    )

    forecast_params = build_forecast_params(locations_df)
    forecast_data = extract_payload(
        url= FORECAST_URL,
        params= forecast_params,
    )

    historical_hourly, historical_daily = transform_data(
        data= historical_data,
        locations = locations,
        run_timestamp= run_timestamp
    )

    forecast_hourly, forecast_daily = transform_data(
        data= forecast_data,
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

    historical_datasets = {
        "historical_hourly": historical_hourly,
        "historical_daily": historical_daily,
    }

    forecast_datasets = {
        "forecast_hourly": forecast_hourly,
        "forecast_daily": forecast_daily,
    }

    connection = create_snowflake_connection()

    try:
        for dataset_name, dataframe in historical_datasets.items():
            partition_dates = get_partition_dates(
                dataframe
            )

            for partition_date in partition_dates:
                replace_snowflake_partition(
                    dataset=dataset_name,
                    partition_date=partition_date,
                    connection=connection,
                )

            print(
                f"Loaded {len(partition_dates)} "
                f"{dataset_name} partitions into Snowflake"
            )

        for dataset_name, dataframe in forecast_datasets.items():
            partition_dates = get_partition_dates(
                dataframe
            )

            replace_forecast_snapshot(
                dataset=dataset_name,
                partition_dates=partition_dates,
                connection=connection,
            )

            print(
                f"Replaced {dataset_name} with "
                f"{len(partition_dates)} forecast partitions"
            )

    finally:
        connection.close()

if __name__ == "__main__":
    main()

    

