from datetime import date
from pipeline.loaders.snowflake_loader import (
    create_snowflake_connection,
    replace_snowflake_partition,
)

def main() -> None:
    connection = create_snowflake_connection()

    try:
        cursor = connection.cursor()

        cursor.execute(
            """
                SELECT
                    CURRENT_USER(),
                    CURRENT_ROLE(),
                    CURRENT_WAREHOUSE(),
                    CURRENT_DATABASE(),
                    CURRENT_SCHEMA()

            """
        )

        result = cursor.fetchone()
        print(f"Snowflake connection successful: {result}")

    finally:
        cursor.close()
        connection.close()

    dataset = "historical_hourly"
    partition_date = "2025-08-01"

    replace_snowflake_partition(
        dataset= dataset,
        partition_date= partition_date,
        connection= connection
    )

    print(f"Successfully replace dataset: { dataset}, "
          f"partition: {partition_date}"
          )


if __name__ == "__main__":
    main()