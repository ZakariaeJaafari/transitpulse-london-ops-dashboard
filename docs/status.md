# Implementation status

**As of:** 2026-10-30

This repo is **spec-first**. Do not assume Compose, dbt, Airflow, FastAPI, or Next.js exist until they are listed as shipped below.

Canonical spec: [PROJECT.md](../PROJECT.md). Phases: [phases.md](./phases.md). Agent briefing: [AGENTS.md](../AGENTS.md).

## Shipped

| Area | What exists |
|------|-------------|
| Product spec | [PROJECT.md](../PROJECT.md), [README.md](../README.md), [LAYOUT.md](../LAYOUT.md) |
| Folder split | `zoomcamp/` (graded) vs `extras/` (portfolio) — READMEs only, no pipeline code |
| Living wiki | This `docs/` folder including [phases.md](./phases.md) |
| Agent context | `AGENTS.md`, `CLAUDE.md`, `.cursor/rules/`, `.cursor/skills/` |
| ADR | [0001-extras-nextjs-fastapi.md](./decisions/0001-extras-nextjs-fastapi.md) — Next.js on FastAPI |
| Local lake (MinIO) | `zoomcamp/docker-compose.yml` (MinIO + Airflow), `zoomcamp/.env.example`, `zoomcamp/lake/upload_bronze.py` → `transitpulse` bronze prefixes |
| Orchestration (local) | `zoomcamp/dags/tube_status.py` — dlt bronze load → dbt `line_status_snapshot` (Compose Airflow on `:8080`, `MINIO_ENDPOINT=http://minio:9000` in container) |
| Python env | `zoomcamp/pyproject.toml` + `uv.lock` (uv) |
| Batch ingest (dlt) | `zoomcamp/ingest/tfl_line_status_dlt.py`, `weather_hourly_dlt.py`, `fetch_raw.py` — dlt → MotherDuck `transitpulse.bronze.*` (`motherduck_token` / `MOTHERDUCK_TOKEN`) |
| Warehouse (local dev) | DuckDB file `zoomcamp/data/transitpulse.duckdb` for dbt **dev** target only (`marts.*` views); not the live bronze path after Lesson 24 |
| Warehouse (MotherDuck) | `transitpulse`: `bronze.*` from dlt; dbt **motherduck** target → `marts.line_status_snapshot`, `marts.weather_hourly` (same database, no local attach) |
| Dashboard (graded) | `zoomcamp/dashboard/` Streamlit (reads MotherDuck `marts.line_status_snapshot`, `marts.weather_hourly`) |
| Sample | `zoomcamp/sample_data/` |
| CI | `.github/workflows/ci.yml` — `uv sync` + `pytest` in `zoomcamp/` on push |
| Cloud IaC (Phase 1) | `zoomcamp/terraform/` applied in **austriaeast** zone 1: Ubuntu VM `transitpulse-zoomcamp` (`Standard_B2s_v2`), VNet, public IP, NSG on subnet `transitpulse-public` (SSH :22 from the operator CIDR only). Log: [azure-vm-move.md](./azure-vm-move.md). |
| Cloud Compose (Phase 1) | On that VM, MinIO and Airflow came up healthy, then DAG `tube_status` run `manual__2026-10-28T20:41:27` succeeded (2026-10-28). UIs from the laptop use SSH tunnels: Airflow `localhost:8081`, MinIO console `localhost:19001`. Log: [azure-vm-move.md](./azure-vm-move.md). VM deallocated after that session. |

## Target (not built yet)

| Area | Target |
|------|--------|
| Local stack (full) | `Makefile`, Redpanda + Flink in Compose |
| Batch ingest (orchestrated) | Additional Airflow DAGs (weather, MotherDuck load, full batch chain) |
| Stream | `zoomcamp/` producers → Redpanda → PyFlink |
| Lake (silver + orchestration) | Airflow-driven bronze/silver on MinIO beyond manual upload |
| Warehouse (cloud) | MotherDuck load orchestrated from Airflow/VM without laptop attach |
| Cloud (Phase 1) | Streamlit on the VM. Deallocate the VM whenever it is not in use. |
| API (extras) | `extras/api/` FastAPI `/health` + `/metrics/*` + `/ops/freshness` |
| UI (extras) | `extras/nextjs/` — browser → FastAPI only |
| ML (extras) | `extras/ml/` — Phase 2+ |
| StopPoint subset | Curated stop list for arrivals stream / API snapshots |

## Phase tracker

| Phase | Intent | Status |
|-------|--------|--------|
| 0 | Spec, LAYOUT, docs, agent context | Shipped |
| 1 Zoomcamp | Vertical slice in `zoomcamp/` + Terraform free VM (`zoomcamp/terraform/`) | In progress (local slice done; VM Compose and `tube_status` succeeded; VM deallocated) |
| 1 extras API | FastAPI metrics in `extras/api/` on gold marts | Not started |
| 1 extras UI | Next.js in `extras/nextjs/` calling FastAPI only | Not started |
| 2 | Feature marts, XGBoost, MLflow, batch scores (`extras/ml/`) | Not started |
| 3 | `POST /predict` + Next.js predict view | Not started |

## Notes for agents

- **Canonical gold** for Streamlit, FastAPI, and (via API) Next.js = MotherDuck dbt marts, not MinIO “gold files.”
- Prefer one thin working slice in **`zoomcamp/`** over standing up every container.
- `cd zoomcamp && docker compose up -d --wait` starts **MinIO + Airflow** (UI `:8080`; Compose healthcheck on `/health`, entrypoint clears stale webserver PIDs on the `airflow_data` volume). Upload bronze from the **host** (`uv run python lake/upload_bronze.py` uses `localhost:9000`). DAG tasks use `http://minio:9000`. Do not add FastAPI or Next.js to that Compose file.
- Phase 1 compute is one Azure VM from a **$100 credit** ([ADR 0002](./decisions/0002-azure-credit-vm.md)). Laptop slice **before** `terraform apply`. Lake stays MinIO — do not add Azure Blob. Deallocate the VM when idle. See [cloud-and-free-demo.md](./cloud-and-free-demo.md).
- Next.js must not embed `MOTHERDUCK_TOKEN`.
