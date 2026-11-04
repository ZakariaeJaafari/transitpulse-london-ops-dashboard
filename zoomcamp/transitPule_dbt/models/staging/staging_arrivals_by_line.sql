{{ config(materialized='table' if target.name == 'motherduck' else 'view') }}

/*
  Bronze columns (DESCRIBE bronze.arrivals_by_line):
  line_id, n, avg_predicted_seconds, obs_date, _dlt_load_id, _dlt_id

  Line summary from Flink aggregate (one row per line_id), not one arrival per row.
  avg_predicted_seconds is the mean of TfL timeToStation predictions, not timetable delay.
  n is how many predictions Flink counted for that line.
*/

select
    line_id,
    n,
    avg_predicted_seconds,
    cast(obs_date as date) as obs_date
from {{ source('bronze', 'arrivals_by_line') }}
