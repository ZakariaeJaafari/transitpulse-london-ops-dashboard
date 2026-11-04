-- Fails when any row is returned: summary n must match event mart row count per line/date.

with event_counts as (
    select
        line_id,
        obs_date,
        count(*) as event_count
    from {{ ref('arrival_updates') }}
    group by line_id, obs_date
),

summary as (
    select
        line_id,
        obs_date,
        n
    from {{ ref('arrivals_by_line') }}
)

select
    coalesce(s.line_id, e.line_id) as line_id,
    coalesce(s.obs_date, e.obs_date) as obs_date,
    s.n as summary_n,
    e.event_count
from summary as s
full outer join event_counts as e
    on s.line_id = e.line_id and s.obs_date = e.obs_date
where s.n is distinct from e.event_count
