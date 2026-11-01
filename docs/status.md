# Implementation status

**As of:** 2026-11-01

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
| Orchestration (local) | `zoomcamp/dags/tube_status.py`, `weather_hourly.py`, and `transit_batch.py` (tube bronze → weather bronze → both marts, `--target motherduck`). `transit_batch` run `manual__2026-11-01T15:42:33+00:00` succeeded (start 15:42:34 UTC, end 15:43:19 UTC). Compose Airflow listens on `:8080`. On this Windows host open **http://127.0.0.1:8080** — `localhost` is IPv6 and `wslrelay` resets that connection. Container `MINIO_ENDPOINT=http://minio:9000`. |
| Python env | `zoomcamp/pyproject.toml` + `uv.lock` (uv) |
| Batch ingest (dlt) | `zoomcamp/ingest/tfl_line_status_dlt.py`, `weather_hourly_dlt.py`, `fetch_raw.py` — dlt → MotherDuck `transitpulse.bronze.*` (`motherduck_token` / `MOTHERDUCK_TOKEN`) |
| Warehouse (local dev) | DuckDB file `zoomcamp/data/transitpulse.duckdb` for dbt **dev** target only (`marts.*` views); not the live bronze path after Lesson 24 |
| Warehouse (MotherDuck) | `transitpulse`: `bronze.*` from dlt; dbt **motherduck** target → `marts.line_status_snapshot`, `marts.weather_hourly` (same database, no local attach) |
| Dashboard (graded) | `zoomcamp/dashboard/` Streamlit (reads MotherDuck `marts.line_status_snapshot`, `marts.weather_hourly`) |
| Sample | `zoomcamp/sample_data/` |
| CI | `.github/workflows/ci.yml` — `uv sync` + `pytest` in `zoomcamp/` on push |
| Makefile | `zoomcamp/Makefile` — `up`, `test`, `dbt-build` (no extras API target) |
| Cloud IaC (Phase 1) | `zoomcamp/terraform/` applied in **austriaeast** zone 1: Ubuntu VM `transitpulse-zoomcamp` (`Standard_B2s_v2`), VNet, public IP, NSG on subnet `transitpulse-public` (SSH :22 from the operator CIDR only). Log: [azure-vm-move.md](./azure-vm-move.md). |
| Cloud Compose (Phase 1) | On that VM, MinIO and Airflow are healthy. `tube_status` run `manual__2026-10-30T19:09:45+00:00` and `weather_hourly` run `manual__2026-10-30T19:09:48+00:00` succeeded (2026-10-30): MotherDuck `transitpulse.bronze` and the two marts. UIs from the laptop use SSH tunnels: Airflow `localhost:8081`, MinIO console `localhost:19001`. On 2026-10-31 Streamlit ran on the VM host (not a Compose service), tunneled at `localhost:8502`, and rendered both MotherDuck tiles. Log: [azure-vm-move.md](./azure-vm-move.md). VM deallocated after that session. |

## Target (not built yet)

| Area | Target |
|------|--------|
| Local stack (full) | Redpanda + Flink in Compose |
| Stream | `zoomcamp/` producers → Redpanda → PyFlink |
| Lake (silver + orchestration) | Airflow-driven bronze/silver on MinIO beyond manual upload |
| API (extras) | `extras/api/` FastAPI `/health` + `/metrics/*` + `/ops/freshness` |
| UI (extras) | `extras/nextjs/` — browser → FastAPI only |
| ML (extras) | `extras/ml/` — Phase 2+ |
| StopPoint subset | Curated stop list for arrivals stream / API snapshots |

## Phase tracker

| Phase | Intent | Status |
|-------|--------|--------|
| 0 | Spec, LAYOUT, docs, agent context | Shipped |
| 1 Zoomcamp | Vertical slice in `zoomcamp/` + Terraform free VM (`zoomcamp/terraform/`) | VM slice done (Compose, `tube_status` and `weather_hourly`, Streamlit on the VM, deallocated 2026-10-31). Laptop `transit_batch` run `manual__2026-11-01T15:42:33+00:00` succeeded. Silver and stream are still open. |
| 1 extras API | FastAPI metrics in `extras/api/` on gold marts | Not started |
| 1 extras UI | Next.js in `extras/nextjs/` calling FastAPI only | Not started |
| 2 | Feature marts, XGBoost, MLflow, batch scores (`extras/ml/`) | Not started |
| 3 | `POST /predict` + Next.js predict view | Not started |

## Notes for agents

- **Canonical gold** for Streamlit, FastAPI, and (via API) Next.js = MotherDuck dbt marts, not MinIO “gold files.”
- Prefer one thin working slice in **`zoomcamp/`** over standing up every container.
- `cd zoomcamp && docker compose up -d --wait` starts **MinIO + Airflow** (container healthcheck on `http://127.0.0.1:8080/health`). On this Windows laptop the browser URL is **http://127.0.0.1:8080**; `http://localhost:8080` is reset by `wslrelay` on IPv6. Upload bronze from the **host** (`uv run python lake/upload_bronze.py` uses `localhost:9000`). DAG tasks use `http://minio:9000`. Do not add FastAPI or Next.js to that Compose file.
- Phase 1 compute is one Azure VM from a **$100 credit** ([ADR 0002](./decisions/0002-azure-credit-vm.md)). Laptop slice **before** `terraform apply`. Lake stays MinIO — do not add Azure Blob. Deallocate the VM when idle. See [cloud-and-free-demo.md](./cloud-and-free-demo.md).
- Next.js must not embed `MOTHERDUCK_TOKEN`.
