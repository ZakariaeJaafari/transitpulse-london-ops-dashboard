# Data model

Target warehouse: MotherDuck database `transitpulse`. Spec detail: [PROJECT.md](../PROJECT.md) §5.

**Canonical gold** for FastAPI and the dashboard is **dbt marts in MotherDuck**. MinIO may hold optional gold-shaped Parquet; do not treat those files as the serving source of truth.

## Conformed keys

Use these names in silver and gold (do not invent parallel IDs):

| Key | Meaning |
|-----|---------|
| `line_id` | TfL line identifier (e.g. Tube line) |
| `stop_id` | StopPoint / station identifier |
| `obs_ts` | Observation timestamp (UTC or `Europe/London` — pick one, document in the model YAML) |
| `event_date` / `status_date` / `obs_date` | Date partition columns derived from timestamps |

## Delay / reliability (define before coding)

TfL arrivals `timeToStation` is a **prediction**, not delay vs timetable.

Until an ADR says otherwise, Phase 1 metrics use an explicit **proxy**, for example:

- disruption share from line status severity, and/or
- predicted wait vs a threshold, labeled as a proxy in the dashboard footnote.

A true delay vs Journey Planner / GTFS scheduled arrival is a later join — do not imply timetable delay if the join is not implemented.

## Target marts

| Table | Grain | Partition / cluster | Why |
|-------|-------|---------------------|-----|
| `marts.fact_arrival_updates` | one prediction/event | Partition `event_date`; sort/cluster `line_id`, `stop_id` | Date filters + line grouping |
| `marts.fact_line_status_hourly` | line × hour | Partition `status_date`; cluster `line_id` | Disruption over time |
| `marts.dim_line` / `dim_stop` | entity | None (small) | Joins |
| `marts.fact_weather_hourly` | London × hour | Partition `obs_date` | Weather overlay |
| `marts.mart_line_reliability_daily` | line × day | Partition by month; cluster `line_id` | Categorical tile |
| `ops.source_freshness` | source × check_ts | — | Ops / reproducibility |

MotherDuck/DuckDB: partition **columns** + intentional `ORDER BY` / indexes. Postgres fallback: `PARTITION BY RANGE (event_date)` and `CLUSTER` / indexes. Write the explanation next to the dbt models, not only in README.

## Volume controls

- Poll a **StopPoint subset** (document IDs in code + `sample_data/`).
- Keep `sample_data/` bronze snippets so the demo runs without live TfL.

## dbt layers

`staging_*` → `int_*` → `marts_*` with `not_null`, `unique`, and `relationships` tests.

### As-built (local DuckDB)

| Object | Grain | Notes |
|--------|-------|--------|
| `marts.line_status_snapshot` | line × `status_date` | TfL line status picker from bronze dlt tables |
| `marts.weather_hourly` | one row per hour | `time` in Europe/London local; Open-Meteo fields from `bronze.weather_hourly` |

Column docs: `zoomcamp/transitPule_dbt/models/marts/schema.yml`. MotherDuck marts not built yet — see [status.md](./status.md).
