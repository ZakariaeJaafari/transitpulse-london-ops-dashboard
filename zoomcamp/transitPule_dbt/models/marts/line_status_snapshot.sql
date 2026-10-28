{{ config(materialized='view') }}

/*
  Bronze columns (DESCRIBE):
  - bronze.tfl_line_status: id, _dlt_id, _dlt_load_id, ...
  - bronze.tfl_line_status__line_statuses: line_id, status_severity,
    status_severity_description, _dlt_id, _dlt_parent_id, ...
  - bronze.tfl_line_status__line_statuses__validity_periods: is_now,
    _dlt_parent_id, ...

  Picker mirrors zoomcamp/ingest/line_status_proxy.py:
  prefer statuses with any validity period is_now; else all line statuses;
  then lowest status_severity. disrupted when severity != 10.
  line_id comes from the parent line row (Good Service rows omit line_id).
*/

with statuses as (
    select
        line.id as line_id,
        load.inserted_at as load_inserted_at,
        status.status_severity,
        status.status_severity_description,
        status._dlt_id as status_dlt_id
    from {{ source('bronze', 'tfl_line_status') }} as line
    inner join {{ source('bronze', 'tfl_line_status__line_statuses') }} as status
        on status._dlt_parent_id = line._dlt_id
    inner join {{ source('bronze', '_dlt_loads') }} as load
        on load.load_id = line._dlt_load_id
),

status_is_now as (
    select
        statuses.*,
        coalesce(max(case when validity.is_now then 1 else 0 end), 0) = 1 as is_current_status
    from statuses
    left join {{ source('bronze', 'tfl_line_status__line_statuses__validity_periods') }} as validity
        on validity._dlt_parent_id = statuses.status_dlt_id
    group by all
),

line_has_current as (
    select
        line_id,
        max(case when is_current_status then 1 else 0 end) = 1 as line_has_current
    from status_is_now
    group by line_id
),

status_pool as (
    select status_is_now.*
    from status_is_now
    inner join line_has_current as line_current using (line_id)
    where
        (line_current.line_has_current and status_is_now.is_current_status)
        or (not line_current.line_has_current)
),

ranked as (
    select
        status_pool.*,
        row_number() over (
            partition by line_id
            order by status_severity asc, status_severity_description, status_dlt_id
        ) as pick_rank
    from status_pool
)

select
    line_id,
    status_severity,
    coalesce(status_severity_description, '') as status_description,
    status_severity != 10 as disrupted,
    cast(load_inserted_at as date) as status_date
from ranked
where pick_rank = 1
