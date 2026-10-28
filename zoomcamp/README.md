# TransitPulse — Zoomcamp scope

This folder is the **DataTalksClub DE Zoomcamp course project**.

## Graded path

Dataset (TfL + Open-Meteo) → **data lake (MinIO)** → **warehouse (MotherDuck)** → **dbt** → **dashboard (2 tiles)**.

Orchestration: Airflow. Batch ingest: **dlt** (+ PySpark as needed). Stream (optional track): Redpanda + PyFlink.

## How to run

Commands below assume you are **in this folder** (`zoomcamp/`).

**As-built (local laptop):**

MinIO lake (API `9000`, console `9001`) + Airflow (`8080`):

```bash
cd zoomcamp
cp .env.example .env   # add MINIO_ROOT_USER / MINIO_ROOT_PASSWORD (and other keys)
docker compose up -d --build --wait   # waits until Airflow /health is OK (~1–3 min first start)
uv sync
uv run python lake/upload_bronze.py   # host → http://localhost:9000
# Console: http://localhost:9001 → bucket transitpulse → bronze/…
# Airflow UI: http://localhost:8080 — login admin / admin (override AIRFLOW_WWW_USER / AIRFLOW_WWW_USER_PASSWORD in .env)
# Unpause DAG tube_status → load_tube_bronze → build_line_status_mart
```

Airflow runs in Docker (not on Windows natively). The repo is mounted at `/opt/transitpulse/zoomcamp`; DAG tasks use `http://minio:9000` for the lake. Keep `MINIO_ENDPOINT=http://localhost:9000` in `.env` for host commands.

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

Tests (no `.env` required):

```bash
cd zoomcamp
uv sync --all-groups
uv run pytest
```

**Target (not all shipped yet):**

```bash
cd zoomcamp
cp .env.example .env   # TFL_APP_KEY, MOTHERDUCK_TOKEN, MinIO keys
docker compose up -d
# Airflow → dlt → dbt → open dashboard/ (2 tiles)
```

`.env.example`, Compose, dbt, and Terraform (`terraform/` — Phase 1 Azure credit VM) all live **here**, not at the repo root.

**Terraform (Azure VM, $100 credit):**

```bash
az login
cd zoomcamp/terraform
cp terraform.tfvars.example terraform.tfvars   # subscription id, your IP, SSH public key; file is gitignored
terraform init
terraform validate
# spends the credit: terraform plan && terraform apply
```

Details: [terraform/README.md](terraform/README.md) · [docs/cloud-and-free-demo.md](../docs/cloud-and-free-demo.md).

The VM is applied (`austriaeast`, zone 1, `Standard_B2s_v2`). Lean Compose on that VM is the next step. Deallocate it when idle.

Living docs: [docs/zoomcamp.md](../docs/zoomcamp.md) · [docs/cloud-and-free-demo.md](../docs/cloud-and-free-demo.md) · [docs/status.md](../docs/status.md)

## Out of scope here

See `../extras/` for FastAPI (backend), Next.js (product UI), ML/`/predict` — **not required for peer grading**.
