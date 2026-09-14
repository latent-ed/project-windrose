import logging
import pandas as pd
from config.config import HISTORY_URL, FORECAST_URL
from pipeline.ingestion.extract import extract_payload
from scripts.api_parameters import build_forecast_params, build_historical_params

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

logger = logging.getLogger(__name__)

location_df = pd.read_csv("config/locations.csv")
locations = location_df.to_dict(orient="records")

def main():
    historical_params = build_historical_params(
        locations_df = location_df,
        start_date = "2025-08-01",
        end_date = "2026-07-31"
        )
    forecast_params = build_forecast_params(
        locations_df = location_df
    )

    historical_payload = extract_payload(HISTORY_URL, historical_params)

    forcast_payload = extract_payload(FORECAST_URL, forecast_params)

    logger.info(
        "Historical extraction successful: %s locations",
        len(historical_payload),
    )

    logger.info(
        "Forecast extraction successful: %s locations",
        len(forcast_payload)
    )

if __name__=="__main__":
    main()
