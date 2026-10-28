# TransitPulse — Zoomcamp scope

This folder is the **DataTalksClub DE Zoomcamp course project**.

## Graded path

Dataset (TfL + Open-Meteo) → **data lake (MinIO)** → **warehouse (MotherDuck)** → **dbt** → **dashboard (2 tiles)**.

Orchestration: Airflow. Batch ingest: **dlt** (+ PySpark as needed). Stream (optional track): Redpanda + PyFlink.

## How to run

Commands below assume you are **in this folder** (`zoomcamp/`).

**As-built (local laptop):**

MinIO lake (API `9000`, console `9001`):

```bash
cd zoomcamp
cp .env.example .env   # add MINIO_ROOT_USER / MINIO_ROOT_PASSWORD (and other keys)
docker compose up -d
uv sync
uv run python lake/upload_bronze.py
# Console: http://localhost:9001 → bucket transitpulse → bronze/…
```

DuckDB warehouse path (no MinIO required):

```bash
cd zoomcamp
uv sync
uv run python ingest/tfl_line_status_dlt.py
uv run python ingest/weather_hourly_dlt.py
cd transitPule_dbt
uv run dbt run --profiles-dir . --select line_status_snapshot weather_hourly
uv run dbt test --profiles-dir . --select line_status_snapshot weather_hourly
cd ..
uv run streamlit run dashboard/app.py
```

Warehouse: `data/transitpulse.duckdb` (`bronze.*`, `marts.*` views). Dashboard reads `marts.*` read-only.

**Target (not all shipped yet):**

```bash
cd zoomcamp
cp .env.example .env   # TFL_APP_KEY, MOTHERDUCK_TOKEN, MinIO keys
docker compose up -d
# Airflow → dlt → dbt → open dashboard/ (2 tiles)
```

`.env.example`, Compose, dbt, and Terraform (`terraform/` — Phase 1 free VM) all live **here**, not at the repo root.

Laptop slice first, then `terraform apply`, then lean Compose on the VM. See [docs/cloud-and-free-demo.md](../docs/cloud-and-free-demo.md).

Living docs: [docs/zoomcamp.md](../docs/zoomcamp.md) · [docs/cloud-and-free-demo.md](../docs/cloud-and-free-demo.md) · [docs/status.md](../docs/status.md)

## Out of scope here

See `../extras/` for FastAPI (backend), Next.js (product UI), ML/`/predict` — **not required for peer grading**.
