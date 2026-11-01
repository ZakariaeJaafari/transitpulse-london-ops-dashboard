{{ config(materialized='table' if target.name == 'motherduck' else 'view') }}
select
    time,
    temperature_2m,
    precipitation,
    wind_speed_10m
from {{ ref('staging_weather_hourly') }}
