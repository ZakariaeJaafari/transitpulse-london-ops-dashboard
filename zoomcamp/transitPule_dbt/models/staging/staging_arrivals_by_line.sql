{{ config(materialized='table' if target.name == 'motherduck' else 'view') }}

/*
  Line summary grouped from staging_arrival_updates (distinct predictions per line).
  avg_predicted_seconds is the mean of TfL timeToStation predictions, not timetable delay.
  n is the count of distinct predictions for that line and obs_date.
*/

select
    line_id,
    cast(count(*) as bigint) as n,
    cast(avg(time_to_station_sec) as double) as avg_predicted_seconds,
    obs_date
from {{ ref('staging_arrival_updates') }}
group by line_id, obs_date
