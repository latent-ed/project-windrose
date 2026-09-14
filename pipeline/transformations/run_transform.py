import pandas as pd
from pipeline.ingestion.run_extract import main
from pipeline.transformations.transform import transform_data
from tests.fixtures.sample_weather_data import TEST_DATA
location_df = pd.read_csv("config/locations.csv")

is_leeds = location_df["location_id"] == "LEEDS"
leeds_df = location_df[is_leeds]
test_locations = leeds_df.to_dict(orient="records")


def main():
    run_timestamp = pd.Timestamp.now(tz= "UTC")
    hourly_data, daily_data = transform_data(
        TEST_DATA,
        test_locations,
        run_timestamp,
    )

    print("\nHourly data:")
    print(hourly_data.head())

    print("\nDaily data:")
    print(daily_data.head())

    print("\nShapes:")
    print(f"Hourly: {hourly_data.shape}")
    print(f"Daily: {daily_data.shape}")

    print("\nHourly columns:")
    print(hourly_data.columns.tolist())

    print("\nDaily columns:")
    print(daily_data.columns.tolist())

if __name__ == "__main__":
    main()