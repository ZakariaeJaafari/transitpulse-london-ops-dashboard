# TransitPulse — Zoomcamp scope

This folder is the **DataTalksClub DE Zoomcamp course project**.

## Graded path

Dataset (TfL + Open-Meteo) → **data lake (MinIO)** → **warehouse (MotherDuck)** → **dbt** → **dashboard (2 tiles)**.

Orchestration: Airflow. Batch ingest: **dlt** (+ PySpark as needed). Stream (optional track): Redpanda + PyFlink.

## How to run

(Fill as you implement.) Commands below assume you are **in this folder** (`zoomcamp/`).

```bash
cd zoomcamp
cp .env.example .env   # TFL_APP_KEY, MOTHERDUCK_TOKEN, MinIO keys
docker compose up -d
# Airflow → dlt → dbt → open dashboard/ (2 tiles)
```

`.env.example`, Compose, dbt, and Terraform (`terraform/` — Phase 1 Cloud 4/4) all live **here**, not at the repo root.

Laptop slice first, then `terraform apply`, then lean Compose on the VM. See [docs/cloud-and-free-demo.md](../docs/cloud-and-free-demo.md).

Living docs: [docs/zoomcamp.md](../docs/zoomcamp.md) · [docs/cloud-and-free-demo.md](../docs/cloud-and-free-demo.md) · [docs/status.md](../docs/status.md)

## Out of scope here

See `../extras/` for FastAPI (backend), Next.js (product UI), ML/`/predict` — **not required for peer grading**.
