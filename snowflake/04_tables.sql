USE ROLE SYSADMIN;
USE DATABASE WINDROSE_DB;
USE SCHEMA WEATHER;

CREATE TABLE IF NOT EXISTS historical_hourly(
    time                    TIMESTAMP_NTZ,
    temperature_2m          FLOAT,
    precipitation           FLOAT,
    wind_speed_10m          FLOAT,
    relative_humidity_2m    FLOAT,    
    location_id             VARCHAR(50),
    city                    VARCHAR(100),
    latitude                NUMBER(9,6),
    longitude               NUMBER(10,6),
    timezone                VARCHAR(50),
    download_date           DATE,
    run_time_stamp          TIMESTAMP_NTZ
);

CREATE TABLE IF NOT EXISTS historical_daily (
    time                    DATE,
    temperature_2m_max      FLOAT,
    temperature_2m_min      FLOAT,
    precipitation_sum       FLOAT,
    sunshine_duration       FLOAT,
    location_id             VARCHAR(50),
    city                    VARCHAR(100),
    latitude                NUMBER(9, 6),
    longitude               NUMBER(10, 6),
    timezone                VARCHAR(50),
    download_date           DATE,
    run_time_stamp          TIMESTAMP_TZ
);

CREATE TABLE IF NOT EXISTS forecast_hourly
LIKE historical_hourly;

CREATE TABLE IF NOT EXISTS forecast_daily
LIKE historical_daily;