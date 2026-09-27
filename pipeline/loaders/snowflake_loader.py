import os

import snowflake.connector
from dotenv import load_dotenv
from datetime import date
from snowflake.connector import SnowflakeConnection


load_dotenv()

DATASET_TABLES = {
    "historical_hourly": "HISTORICAL_HOURLY",
    "historical_daily": "HISTORICAL_DAILY",
    "forecast_hourly": "FORECAST_HOURLY",
    "forecast_daily": "FORECAST_DAILY",
}

def get_required_env(variable_name: str) -> str:
    value = os.getenv(variable_name)

    if not value:
        raise ValueError(
            f"Required environment variable is missing: {variable_name}"
        )
    return value

def create_snowflake_connection() -> SnowflakeConnection:
    connection = snowflake.connector.connect(
        account = get_required_env("SNOWFLAKE_ACCOUNT"),
        user = get_required_env("SNOWFLAKE_USER"),
        password = get_required_env("SNOWFLAKE_PASSWORD"),
        warehouse = get_required_env("SNOWFLAKE_WAREHOUSE"),
        database = get_required_env("SNOWFLAKE_DATABASE"),
        schema = get_required_env("SNOWFLAKE_SCHEMA"),
        role = get_required_env("SNOWFLAKE_ROLE")

    )

    return connection

def replace_snowflake_partition(
    dataset: str,
    partition_date: str,
    connection: SnowflakeConnection,
) -> None:
    if dataset not in DATASET_TABLES:
        raise ValueError(f"Invalid dataset: {dataset}")

    try:
        validated_date = date.fromisoformat(partition_date)
    except ValueError as error:
        raise ValueError(
            "partition_date must use YYYY-MM-DD format"
        ) from error

    table_name = DATASET_TABLES[dataset]

    stage_path = (
    f"@WINDROSE_S3_STAGE/"
    f"{dataset}/"
    f"dt={validated_date.isoformat()}/"
)
    
    cursor = connection.cursor()

    try:
        cursor.execute("BEGIN TRANSACTION")

        delete_sql = f"""
            DELETE FROM {table_name}
            WHERE TO_DATE(time) = %s
        """
        cursor.execute(delete_sql, (validated_date,))

        copy_sql = f"""
            COPY INTO {table_name}
            FROM {stage_path}
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            ON_ERROR = ABORT_STATEMENT
            FORCE = TRUE
        """

        cursor.execute(copy_sql)

        connection.commit()

    except Exception:
        connection.rollback()
        raise

    finally:
        cursor.close()


FORECAST_DATASETS = {
    "forecast_hourly",
    "forecast_daily",
}


def replace_forecast_snapshot(
    dataset: str,
    partition_dates: list[str],
    connection: SnowflakeConnection,
) -> None:
    if dataset not in FORECAST_DATASETS:
        raise ValueError(
            f"Invalid forecast dataset: {dataset}"
        )

    if not partition_dates:
        raise ValueError(
            "No forecast partition dates were supplied"
        )

    validated_dates = []

    for partition_date in partition_dates:
        try:
            validated_date = date.fromisoformat(
                partition_date
            )
        except ValueError as error:
            raise ValueError(
                "partition dates must use YYYY-MM-DD format"
            ) from error

        validated_dates.append(validated_date)

    table_name = DATASET_TABLES[dataset]
    cursor = connection.cursor()

    try:
        cursor.execute("BEGIN TRANSACTION")

        delete_sql = f"DELETE FROM {table_name}"
        cursor.execute(delete_sql)

        for validated_date in validated_dates:
            stage_path = (
                f"@WINDROSE_S3_STAGE/"
                f"{dataset}/"
                f"dt={validated_date.isoformat()}/"
            )

            copy_sql = f"""
                COPY INTO {table_name}
                FROM {stage_path}
                MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                ON_ERROR = ABORT_STATEMENT
                FORCE = TRUE
            """

            cursor.execute(copy_sql)

        connection.commit()

    except Exception:
        connection.rollback()
        raise

    finally:
        cursor.close()
        


    