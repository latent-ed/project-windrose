def build_coordinates(locations_df):
    return {
        "latitude": locations_df["latitude"].tolist(),
        "longitude": locations_df["longitude"].tolist(),
        "timezone": "Europe/London",
    }

def build_historical_params(
    locations_df,
    start_date,
    end_date,
):
    coordinates = build_coordinates(locations_df)

    return {
        **coordinates,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": [
            "temperature_2m",
            "precipitation",
            "wind_speed_10m",
            "relative_humidity_2m",
        ],
        "daily": [
            "temperature_2m_max",
            "temperature_2m_min",
            "precipitation_sum",
            "sunshine_duration",
],
    }

def build_forecast_params(locations_df):
    coordinates = build_coordinates(locations_df)

    return {
        **coordinates,
        "forecast_days": 7,
        "hourly": [
            "temperature_2m",
            "precipitation",
            "wind_speed_10m",
            "relative_humidity_2m",
        ],
        "daily": [
            "temperature_2m_max",
            "temperature_2m_min",
            "precipitation_sum",
            "sunshine_duration",
        ],
    }