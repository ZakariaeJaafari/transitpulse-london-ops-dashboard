---
name: data-contracts
description: Keeps TransitPulse schemas, dbt marts, FastAPI payloads, and dashboard queries on one contract (keys, grains, partition/cluster, delay proxy, StopPoint subset). Use when changing schemas, dbt models, API JSON, or dashboard queries.
---

# Data contracts

Source of truth for grains: [docs/data-model.md](../../../docs/data-model.md) and [PROJECT.md](../../../PROJECT.md) §5.

## Keys (do not rename)

`line_id`, `stop_id`, `obs_ts`, plus date partitions `event_date` / `status_date` / `obs_date`.

## Delay proxy

`timeToStation` is **not** delay vs schedule. Before exposing a “delay” field:

1. Document the proxy (e.g. line-status disruption share, or predicted wait vs threshold) in `docs/data-model.md`, **or**
2. Implement a timetable/GTFS join and record an ADR.

Dashboard footnotes must not claim timetable delay if the join is missing.

## Target marts

| Table | Grain | Partition / cluster |
|-------|-------|---------------------|
| `marts.fact_arrival_updates` | one prediction/event | `event_date` / `line_id`, `stop_id` |
| `marts.fact_line_status_hourly` | line × hour | `status_date` / `line_id` |
| `marts.dim_line`, `dim_stop` | entity | none |
| `marts.fact_weather_hourly` | London × hour | `obs_date` |
| `marts.mart_line_reliability_daily` | line × day | month / `line_id` |
| `ops.source_freshness` | source × check_ts | — |

Write partition/cluster **why** in dbt YAML. `zoomcamp/dashboard/` and FastAPI (`extras/api/`) must use these grains (or a documented superset), not ad-hoc aggregates from bronze. Zoomcamp tiles **query marts directly** — they must not depend on `extras/api/`.

## Volume

- Fixed StopPoint ID list in config + `zoomcamp/sample_data/`.
- London weather: lat `51.5074`, lon `-0.1278` unless an ADR picks a borough centroid.

## After schema changes

Update `docs/data-model.md`, `docs/serving.md` if payloads/tiles change, and `docs/status.md`.
