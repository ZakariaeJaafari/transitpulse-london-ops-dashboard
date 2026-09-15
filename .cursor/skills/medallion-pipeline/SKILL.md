---
name: medallion-pipeline
description: Implements TransitPulse bronze/silver/gold paths (dlt, Airflow, Spark, Redpanda, PyFlink, MinIO). Use when adding producers, Airflow DAGs, dlt pipelines, Spark or Flink jobs, or MinIO prefixes.
---

# Medallion pipeline

Code goes in **`zoomcamp/`**. Layers and prefixes: [docs/pipelines.md](../../../docs/pipelines.md).

## Layers

| Layer | What | Where |
|-------|------|--------|
| Bronze | Raw JSON / untouched ZIPs | MinIO `s3://transitpulse/bronze/<source>/<yyyy>/<mm>/<dd>/` |
| Silver | Typed Parquet, keys `line_id`, `stop_id`, `obs_ts` | MinIO `s3://transitpulse/silver/<entity>/` |
| Gold | dbt marts | **MotherDuck** (canonical). Optional MinIO gold files are not the API source |

## Tools (do not confuse)

- **dlt** — batch/API ingest into bronze or warehouse landing (Airflow runs it).
- **dbt** — SQL transforms in MotherDuck.
- **PySpark** — heavy parse/joins on large extracts → silver.
- **Producers + Redpanda + PyFlink** — live arrivals / line status / weather.

## Rules

1. Do not silently “fix” bronze. Cleaning happens in silver or dbt staging.
2. Idempotent writes; partition by date on object prefixes.
3. Poll a **StopPoint subset**; commit examples under `zoomcamp/sample_data/`.
4. Phase 1: one path end-to-end before adding the other ingest mode as a demo extra.
5. After path or prefix changes, update `docs/pipelines.md` + `docs/status.md` (skill `update-docs`).

## Batch vs stream (target)

- Batch: Airflow → dlt (TfL files + Open-Meteo) → optional Spark silver → load MD → dbt.
- Stream: producers → topics `tfl.arrivals`, `tfl.line_status`, `weather.london` → PyFlink windows → MinIO / staging.

Windows that emit “delay” must use the **proxy** defined in `docs/data-model.md`, not implied timetable delay.
