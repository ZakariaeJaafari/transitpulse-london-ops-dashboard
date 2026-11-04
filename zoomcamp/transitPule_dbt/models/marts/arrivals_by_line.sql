{{ config(materialized='table' if target.name == 'motherduck' else 'view') }}
select
    line_id,
    n,
    avg_predicted_seconds,
    obs_date
from {{ ref('staging_arrivals_by_line') }}
