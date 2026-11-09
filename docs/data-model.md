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
| `silver/arrivals_by_line/obs_date=2026-11-01/arrivals_by_line.json` | one row per `line_id` | JSON lines copied from the newest Flink `part-*` (`n`, `avg_predicted_seconds`). `avg_predicted_seconds` is the mean of TfL `timeToStation` predictions, not timetable delay. Partition `obs_date`. Left in place when Parquet is written. |
| `silver/arrivals_by_line/obs_date=2026-11-01/arrivals_by_line.parquet` | one row per `line_id` | Typed Parquet from that JSON via DuckDB `read_ndjson`. Columns `line_id` VARCHAR, `n` BIGINT, `avg_predicted_seconds` DOUBLE (still TfL predicted seconds, not timetable delay). |

### As-built (MotherDuck `transitpulse`)

| Object | Grain | Notes |
|--------|-------|--------|
| `marts.staging_line_status` | line × `status_date` | Tube picker from bronze dlt tables. Table in schema `marts` on the `motherduck` target. |
| `marts.line_status_snapshot` | line × `status_date` | `select` from `staging_line_status`. Date filter `status_date`; group key `line_id`. |
| `marts.staging_weather_hourly` | one row per hour | Cast `bronze.weather_hourly.time` to Europe/London wall-clock `timestamp`; passes through Open-Meteo measure columns. Table in schema `marts` on the `motherduck` target. |
| `marts.weather_hourly` | one row per hour | `select` from `staging_weather_hourly` (`time`, `temperature_2m`, `precipitation`, `wind_speed_10m`). |
| `bronze.arrivals_by_line` | one row per `line_id` | dlt `replace` from silver `arrivals_by_line.parquet`. Columns `line_id`, `n`, `avg_predicted_seconds`, `obs_date`. `obs_date` is `2026-11-01` from the object path (not a Parquet column). Line summary, not one arrival per row. `avg_predicted_seconds` is still TfL predicted seconds, not timetable delay. |
| `marts.staging_arrivals_by_line` | one row per `line_id` × `obs_date` | `GROUP BY` from `staging_arrival_updates` (`COUNT` → `n`, `AVG(time_to_station_sec)` → `avg_predicted_seconds`). Table in schema `marts` on the `motherduck` target. |
| `marts.arrivals_by_line` | one row per `line_id` × `obs_date` | `select` from `staging_arrivals_by_line`. `avg_predicted_seconds` is mean TfL predicted seconds, not timetable delay; `n` is distinct prediction count. Dashboard reads this mart. On 2026-11-09 `dbt show` still returned Victoria `n=1` average 65 and Bakerloo `n=1` average 123 for `obs_date` 2026-11-01. The local file `stream/output/arrival_updates_live/` is not this table. |
| `bronze.arrival_updates` | one row per prediction event | dlt `replace` from silver `arrival_updates.parquet` (four rows including duplicate pair from double publish). `time_to_station_sec` is TfL predicted seconds, not timetable delay. |
| `marts.staging_arrival_updates` | one distinct prediction | `select distinct` from `bronze.arrival_updates` (drops duplicate publish); omits `_dlt_*`. Table in schema `marts` on the `motherduck` target. |
| `marts.arrival_updates` | one distinct prediction | `select` from `staging_arrival_updates`. Event grain for future fact-style use; `time_to_station_sec` still TfL timeToStation. |

Column docs and mart tests: `zoomcamp/transitPule_dbt/models/marts/schema.yml` (tests apply to the marts, not staging). All four objects are tables in MotherDuck on the `motherduck` target. The dbt `dev` target still builds views in the local DuckDB file. See [status.md](./status.md).
