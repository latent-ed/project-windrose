import os
from io import BytesIO

import boto3
import pandas as pd
from dotenv import load_dotenv

load_dotenv()
s3_client = boto3.client("s3")

bucket_name = os.getenv("S3_BUCKET_NAME")
aws_region = os.getenv("AWS_REGION")
environment = os.getenv("ENVIRONMENT")

def upload_dataframe_to_s3(
    data: pd.DataFrame,
    dataset: str,
) -> list[str]:
    
    uploaded_paths = []

    allowed_datasets = {
    "historical_hourly",
    "historical_daily",
    "forecast_hourly",
    "forecast_daily",
}
    if not isinstance(data, pd.DataFrame):
        raise TypeError("data must be a pandas DataFrame")

    if data.empty:
        raise ValueError("cannot upload a empty DataFrame")

    if dataset not in allowed_datasets:
        raise ValueError(f"Invalid dataset: {dataset}" )
    
    if "time" not in data.columns:
        raise ValueError("DataFrame must contain a time column")

    working_data = data.copy()

    if dataset.endswith("_hourly"):
        working_data["partition_date"] = working_data["time"].dt.date
    else:
        working_data["partition_date"] = working_data["time"]

    grouped_by_date = working_data.groupby("partition_date")

    for partition_date, date_dataframe in grouped_by_date:
        date_dataframe = date_dataframe.drop(columns="partition_date")

        s3_key = (
            f"{environment}/openmeteo/{dataset}/dt={partition_date}/data.parquet"
        )

        parquet_data = date_dataframe.to_parquet(
            index=False,
            engine="pyarrow",
        )


        s3_client.put_object(
            Bucket = bucket_name,
            Key=s3_key,
            Body=parquet_data
        )

        s3_uri = f"s3://{bucket_name}/{s3_key}"
        uploaded_paths.append(s3_uri)

    return uploaded_paths
    

    

    
    




