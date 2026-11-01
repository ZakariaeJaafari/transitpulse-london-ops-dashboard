{{ config(materialized='table' if target.name == 'motherduck' else 'view') }}

/*
  Bronze columns (DESCRIBE bronze.weather_hourly):
  time, temperature_2m, precipitation, wind_speed_10m, _dlt_load_id, _dlt_id

  Open-Meteo hourly.time values are Europe/London local (naive ISO strings in JSON).
  dlt stores time as TIMESTAMPTZ; convert back to London wall-clock timestamp.
*/

select
    cast("time" at time zone 'Europe/London' as timestamp) as time,
    temperature_2m,
    precipitation,
    wind_speed_10m
from {{ source('bronze', 'weather_hourly') }}
