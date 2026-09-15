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

## Vertical slice (Phase 1)

Ship **one** working path end-to-end before standing up every container:

ingest (dlt **or** stream) → MinIO silver (or MD staging) → dbt marts → **2 dashboard tiles** in `zoomcamp/dashboard/`.

Do **not** block the Zoomcamp slice on FastAPI. Optional later: `extras/api/` `/metrics` against the same marts.

Full stack on a free VM will not fit; lean Compose is documented in [cloud-and-free-demo.md](./cloud-and-free-demo.md).

Code lives under **`zoomcamp/`** (see [LAYOUT.md](../LAYOUT.md)). FastAPI, Next.js, and ML stay in **`extras/`**. Next.js does not belong in the Phase 1 Zoomcamp slice.

As-built: none — see [status.md](./status.md).
