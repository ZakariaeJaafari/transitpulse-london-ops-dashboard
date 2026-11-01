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

TfL arrivals `timeToStation` is a **prediction** (seconds until the train is expected at the stop), not delay vs timetable. In code and marts use **`time_to_station_sec`** when normalized from the API field `timeToStation`.

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

- Poll a **StopPoint subset** only — canonical list in `zoomcamp/ingest/stoppoints.py` (`STOPPOINTS`) and mirror `zoomcamp/sample_data/stoppoints.json`. Each `stop_id` is the TfL **naptanId**.
- Keep other `sample_data/` bronze snippets so the demo runs without live TfL.

| `stop_id` | Name |
|-----------|------|
| `940GZZLUOXC` | Oxford Circus |
| `940GZZLUKSX` | King's Cross St. Pancras |
| `940GZZLUWLO` | Waterloo |
| `940GZZLUVIC` | Victoria |

### Arrival prediction rows (ingest proxy)

`zoomcamp/ingest/arrivals_proxy.py` → `arrival_rows()` keeps **tube** predictions whose `naptanId` is in `STOPPOINTS`. Offline fixture: `sample_data/arrivals_oxford_circus.json` (Oxford Circus snapshot, 2026-11-01 17:13 UTC).

| Field | Source |
|-------|--------|
| `stop_id` | `naptanId` |
| `line_id` | `lineId` |
| `vehicle_id` | `vehicleId` |
| `expected_arrival` | `expectedArrival` |
| `time_to_station_sec` | `timeToStation` (int) |
| `towards` | `towards` |

## dbt layers

`staging_*` → `int_*` → `marts_*` with `not_null`, `unique`, and `relationships` tests.

### As-built silver (MinIO)

| Path | Grain | Notes |
|------|-------|--------|
| `silver/line_status/status_date=2026-10-27/line_status.parquet` | line × `obs_ts` | Typed Parquet from bronze via `line_status_proxy.line_status_row`; no `stop_id` |

### As-built (MotherDuck `transitpulse`)

| Object | Grain | Notes |
|--------|-------|--------|
| `marts.staging_line_status` | line × `status_date` | Tube picker from bronze dlt tables. Table in schema `marts` on the `motherduck` target. |
| `marts.line_status_snapshot` | line × `status_date` | `select` from `staging_line_status`. Date filter `status_date`; group key `line_id`. |
| `marts.staging_weather_hourly` | one row per hour | Cast `bronze.weather_hourly.time` to Europe/London wall-clock `timestamp`; passes through Open-Meteo measure columns. Table in schema `marts` on the `motherduck` target. |
| `marts.weather_hourly` | one row per hour | `select` from `staging_weather_hourly` (`time`, `temperature_2m`, `precipitation`, `wind_speed_10m`). |

Column docs and mart tests: `zoomcamp/transitPule_dbt/models/marts/schema.yml` (tests apply to the marts, not staging). All four objects are tables in MotherDuck on the `motherduck` target. The dbt `dev` target still builds views in the local DuckDB file. See [status.md](./status.md).
