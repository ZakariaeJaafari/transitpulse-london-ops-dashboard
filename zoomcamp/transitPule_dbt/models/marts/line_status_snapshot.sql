{{ config(materialized='table' if target.name == 'motherduck' else 'view') }}
select
    line_id,
    status_severity,
    status_description,
    disrupted,
    status_date
from {{ ref('staging_line_status') }}
