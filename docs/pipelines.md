# Pipelines

Zoomcamp grades **batch or stream**. This project **targets both**; README must say which path is primary for graders. Spec: [PROJECT.md](../PROJECT.md) §3.

**dlt** = ingest/load (Python sources → MinIO / MotherDuck). **dbt** = SQL transforms **inside** the warehouse. Do not mix the names.

## Batch (Airflow + dlt + PySpark)

Target DAG steps:

1. `ingest_tfl_static_dlt` — Journey Planner ZIP / station GTFS → MinIO bronze (and/or MD landing).
2. `ingest_open_meteo_dlt` — Open-Meteo archive/forecast → bronze/staging, incremental by date.
3. `ingest_tfl_api_snapshot_dlt` — optional Unified API snapshot (complements stream).
4. `spark_parse_static_network` — heavy parse/joins → silver Parquet on MinIO.
5. `load_motherduck` — promote silver if dlt did not already land there.
6. `dbt_build` — `dbt run` / `dbt test` on MotherDuck.

## Stream (Redpanda + PyFlink)

1. `tfl_arrivals_producer` — selected StopPoint IDs + line status → topics `tfl.arrivals`, `tfl.line_status`.
2. `weather_poll_producer` — Open-Meteo current → `weather.london`.
3. PyFlink — windows (e.g. 5-min delay **proxies** / status severity) → MinIO bronze/silver and/or staging upserts.

## Medallion on MinIO

Suggested prefixes (adjust in an ADR if the first implementation differs, then update this page):

```
s3://transitpulse/bronze/<source>/<yyyy>/<mm>/<dd>/
s3://transitpulse/silver/<entity>/
```

| Layer | Contents |
|-------|----------|
| **Bronze** | Raw API JSON, untouched ZIPs — no silent schema “fixes” |
| **Silver** | Typed Parquet, conformed `line_id`, `stop_id`, `obs_ts` |
| **Gold** | dbt marts in **MotherDuck** (canonical). Optional gold files on MinIO are not the API source. |

## As-built orchestration (Compose Airflow)

DAG `tube_status` (`zoomcamp/dags/tube_status.py`):

1. `load_tube_bronze` — `uv run python ingest/tfl_line_status_dlt.py` (reads MinIO bronze; in Compose `MINIO_ENDPOINT=http://minio:9000`).
2. `build_line_status_mart` — `uv run dbt run --profiles-dir . --select line_status_snapshot` in `transitPule_dbt`.

Prerequisite: host `uv run python lake/upload_bronze.py` after MinIO is up (host endpoint `http://localhost:9000`).

## As-built scripts (local DuckDB)

| Script | Source | Destination |
|--------|--------|-------------|
| `zoomcamp/lake/upload_bronze.py` | `sample_data/tube_line.json`, `sample_data/london_weather.json` | MinIO `s3://transitpulse/bronze/.../2026/10/27/` (raw bytes) |
| `zoomcamp/ingest/tfl_line_status_dlt.py` | `sample_data/tube_line_2026-10-27.json` | `bronze.tfl_line_status` (+ nested dlt tables) |
| `zoomcamp/ingest/weather_hourly_dlt.py` | `sample_data/london_weather_2026-10-27.json` | `bronze.weather_hourly` (one row per hourly array index) |

dbt marts: `zoomcamp/transitPule_dbt/models/marts/`.

## Vertical slice (Phase 1)

Ship **one** working path end-to-end before standing up every container:

ingest (dlt **or** stream) → MinIO silver (or MD staging) → dbt marts → **2 dashboard tiles** in `zoomcamp/dashboard/`.

Do **not** block the Zoomcamp slice on FastAPI. Optional later: `extras/api/` `/metrics` against the same marts.

Full stack on a free VM will not fit; lean Compose is documented in [cloud-and-free-demo.md](./cloud-and-free-demo.md).

Code lives under **`zoomcamp/`** (see [LAYOUT.md](../LAYOUT.md)). FastAPI, Next.js, and ML stay in **`extras/`**. Next.js does not belong in the Phase 1 Zoomcamp slice.

As-built: local dlt scripts + dbt marts in DuckDB — see [status.md](./status.md).
