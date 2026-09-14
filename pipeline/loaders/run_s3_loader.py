import pandas as pd
from tests.fixtures.sample_weather_data import TEST_DATA
from pipeline.loaders.s3_loader import upload_dataframe_to_s3
from pipeline.transformations.transform import transform_data

location_df = pd.read_csv("config/locations.csv")

test_location = (location_df[location_df["location_id"]=="LEEDS"].to_dict(orient="records"))

def main():

    hourly_tranformed, daily_transformed = transform_data(
        TEST_DATA, test_location
    )
    hourly_uris = upload_dataframe_to_s3(
        hourly_tranformed,
        "historical_hourly"
    )

    daily_uris = upload_dataframe_to_s3(
        daily_transformed,
        "historical_daily"
    )

    print("Uploaded hourly files:", hourly_uris)
    print("Uploaded daily files:", daily_uris)


if __name__ == "__main__":
    main()