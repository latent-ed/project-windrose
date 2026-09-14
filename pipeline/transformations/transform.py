import pandas as pd

def transform_data(data, locations, run_timestamp):
    if not data:
        raise ValueError("No Api data was supplied for transformation")
    if len(data) != len(locations):
        raise ValueError(
            "Number of Api reponses does not match configured locations"
        )
    hourly_list = []
    daily_list = []
    download_date = run_timestamp.date()
    for location,city_payload in zip(locations, data):
        hourly_df = pd.DataFrame(city_payload["hourly"])
        daily_df = pd.DataFrame(city_payload["daily"])

        hourly_df["time"] = pd.to_datetime(hourly_df["time"], errors = "raise")
        daily_df["time"] = pd.to_datetime(daily_df["time"], errors = "raise").dt.date

        hourly_df["location_id"] = location["location_id"]
        hourly_df["city"] = location["city"]
        hourly_df["latitude"] = city_payload["latitude"]
        hourly_df["longitude"]= city_payload["longitude"]
        hourly_df["timezone"]= city_payload["timezone"]
        hourly_df["DOWNLOAD_DATE"] = download_date
        hourly_df["RUN_TIME_STAMP"] = run_timestamp 

        daily_df["location_id"]= location["location_id"]
        daily_df["city"] = location["city"]
        daily_df["latitude"] = city_payload["latitude"]
        daily_df["longitude"]= city_payload["longitude"]
        daily_df["timezone"]= city_payload["timezone"]
        daily_df["DOWNLOAD_DATE"] = download_date
        daily_df["RUN_TIME_STAMP"] = run_timestamp

        hourly_list.append(hourly_df)
        daily_list.append(daily_df)

    hourly_data = pd.concat(hourly_list, ignore_index= True)
    daily_data = pd.concat(daily_list, ignore_index= True)

    return hourly_data, daily_data




    