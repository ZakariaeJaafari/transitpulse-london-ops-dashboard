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
| Local lake (MinIO) | `zoomcamp/docker-compose.yml` (MinIO + Airflow), `zoomcamp/.env.example`, `zoomcamp/lake/upload_bronze.py` → bronze prefixes; `zoomcamp/lake/line_status_to_silver.py` → typed silver Parquet (host or `transit_batch` task `write_line_status_silver`). Gold stays MotherDuck marts. |
| Orchestration (local) | `zoomcamp/dags/tube_status.py`, `weather_hourly.py`, and `transit_batch.py`. `transit_batch`: `load_tube_bronze` → `write_line_status_silver` → `load_weather_bronze` → `build_line_status_mart` (`--select +line_status_snapshot --no-partial-parse`) → `build_weather_mart` (`--select +weather_hourly --no-partial-parse`). `weather_hourly` DAG run `manual__2026-11-01T16:57:47+00:00` succeeded; dbt built `staging_weather_hourly` then `weather_hourly` (16:58:05 UTC, log path `/opt/transitpulse`). `transit_batch` run `manual__2026-11-01T17:00:43+00:00` succeeded (start 17:00:44 UTC, end 17:01:25 UTC); Tube dbt task built `staging_line_status` then `line_status_snapshot`, weather dbt task built `staging_weather_hourly` then `weather_hourly`. Earlier laptop runs: `manual__2026-11-01T16:42:06+00:00` (Tube staging mart only), `manual__2026-11-01T16:20:55+00:00` (with silver task), `manual__2026-11-01T15:42:33+00:00` (before silver). Compose Airflow `:8080`; on this Windows host use **http://127.0.0.1:8080**. Container `MINIO_ENDPOINT=http://minio:9000`. |
| Python env | `zoomcamp/pyproject.toml` + `uv.lock` (uv) |
| Batch ingest (dlt) | `zoomcamp/ingest/tfl_line_status_dlt.py`, `weather_hourly_dlt.py`, `fetch_raw.py` — dlt → MotherDuck `transitpulse.bronze.*` (`motherduck_token` / `MOTHERDUCK_TOKEN`) |
| Warehouse (local dev) | DuckDB file `zoomcamp/data/transitpulse.duckdb` for dbt **dev** target only (`marts.*` views); not the live bronze path after Lesson 24 |
| Warehouse (MotherDuck) | `transitpulse`: `bronze.*` from dlt. dbt **motherduck** target builds `marts.staging_line_status` → `marts.line_status_snapshot` and `marts.staging_weather_hourly` → `marts.weather_hourly` (thin marts select from staging). `make dbt-build` on 2026-11-01: run PASS=4, test PASS=6. No local attach. |
| Dashboard (graded) | `zoomcamp/dashboard/` Streamlit (reads MotherDuck `marts.line_status_snapshot`, `marts.weather_hourly`) |
| Sample | `zoomcamp/sample_data/` (includes `stoppoints.json` mirroring `ingest/stoppoints.py`) |
| StopPoint subset | `zoomcamp/ingest/stoppoints.py` — four curated naptanIds for future arrivals stream; `tests/test_stoppoints.py` |
| Arrivals proxy (offline) | `zoomcamp/ingest/arrivals_proxy.py` + `sample_data/arrivals_oxford_circus.json`; `tests/test_arrivals_proxy.py` — tube-only, subset stops; `time_to_station_sec` is TfL prediction, not delay |
| Arrivals poll (pure) | `zoomcamp/ingest/tfl_arrivals_producer.py` — `poll_arrivals`, `publish_arrivals(messages, send)` (partition key `stop_id`); `tests/test_tfl_arrivals_producer.py` (injected fetch/send, fixed `obs_ts`) |
| CI | `.github/workflows/ci.yml` — `uv sync` + `pytest` in `zoomcamp/` on push |
| Makefile | `zoomcamp/Makefile` — `up`, `test`, `dbt-build` (no extras API target). On Windows, `dbt-build` uses `bash.exe` and loads `.env` from the Makefile directory. The run select is `+line_status_snapshot +weather_hourly`. |
| Cloud IaC (Phase 1) | `zoomcamp/terraform/` applied in **austriaeast** zone 1: Ubuntu VM `transitpulse-zoomcamp` (`Standard_B2s_v2`), VNet, public IP, NSG on subnet `transitpulse-public` (SSH :22 from the operator CIDR only). Log: [azure-vm-move.md](./azure-vm-move.md). |
| Cloud Compose (Phase 1) | On that VM, MinIO and Airflow are healthy. `tube_status` run `manual__2026-10-30T19:09:45+00:00` and `weather_hourly` run `manual__2026-10-30T19:09:48+00:00` succeeded (2026-10-30): MotherDuck `transitpulse.bronze` and the two marts. UIs from the laptop use SSH tunnels: Airflow `localhost:8081`, MinIO console `localhost:19001`. On 2026-10-31 Streamlit ran on the VM host (not a Compose service), tunneled at `localhost:8502`, and rendered both MotherDuck tiles. Log: [azure-vm-move.md](./azure-vm-move.md). VM deallocated after that session. |

## Target (not built yet)

| Area | Target |
|------|--------|
| Local stack (full) | Redpanda + Flink in Compose |
| Stream | `zoomcamp/` producers → Redpanda → PyFlink |
| API (extras) | `extras/api/` FastAPI `/health` + `/metrics/*` + `/ops/freshness` |
| UI (extras) | `extras/nextjs/` — browser → FastAPI only |
| ML (extras) | `extras/ml/` — Phase 2+ |

## Phase tracker

| Phase | Intent | Status |
|-------|--------|--------|
| 0 | Spec, LAYOUT, docs, agent context | Shipped |
| 1 Zoomcamp | Vertical slice in `zoomcamp/` + Terraform free VM (`zoomcamp/terraform/`) | VM slice done (Compose, `tube_status` and `weather_hourly`, Streamlit on the VM, deallocated 2026-10-31). Laptop `transit_batch` run `manual__2026-11-01T17:00:43+00:00` built MinIO silver, both bronze loads, and all four dbt tables (two staging + two marts). Stream still open. |
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
