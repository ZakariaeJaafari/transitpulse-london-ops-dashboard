{{ config(materialized='table' if target.name == 'motherduck' else 'view') }}

select
    stop_id,
    line_id,
    vehicle_id,
    expected_arrival,
    time_to_station_sec,
    towards,
    obs_ts,
    obs_date
from {{ ref('staging_arrival_updates') }}
