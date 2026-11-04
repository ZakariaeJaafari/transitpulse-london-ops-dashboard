{{ config(materialized='table' if target.name == 'motherduck' else 'view') }}

/*
  Bronze columns (DESCRIBE bronze.arrival_updates):
  stop_id, line_id, vehicle_id, expected_arrival, time_to_station_sec, towards,
  obs_ts, obs_date, _dlt_load_id, _dlt_id

  One row per distinct prediction. DISTINCT collapses the duplicate sample publish
  (two identical Kafka batches). time_to_station_sec is TfL timeToStation, not
  timetable delay.
*/

select distinct
    stop_id,
    line_id,
    vehicle_id,
    expected_arrival,
    time_to_station_sec,
    towards,
    obs_ts,
    cast(obs_date as date) as obs_date
from {{ source('bronze', 'arrival_updates') }}
