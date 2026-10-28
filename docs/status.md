# Implementation status

**As of:** 2026-10-27

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
| Local lake (MinIO) | `zoomcamp/docker-compose.yml` (MinIO only), `zoomcamp/.env.example`, `zoomcamp/lake/upload_bronze.py` → `transitpulse` bronze prefixes |
| Python env | `zoomcamp/pyproject.toml` + `uv.lock` (uv) |
| Batch ingest (DuckDB) | `zoomcamp/ingest/tfl_line_status_dlt.py`, `weather_hourly_dlt.py`, `fetch_raw.py` |
| Warehouse (local) | DuckDB `zoomcamp/data/transitpulse.duckdb`, dbt project `zoomcamp/transitPule_dbt/` |
| Dashboard (graded) | `zoomcamp/dashboard/` Streamlit (reads local `marts.*`) |
| Sample | `zoomcamp/sample_data/` |

## Target (not built yet)

| Area | Target |
|------|--------|
| Local stack (full) | `Makefile`, Airflow + Redpanda + Flink in Compose |
| Batch ingest (orchestrated) | `zoomcamp/` Airflow DAGs wrapping **dlt** → MinIO / MotherDuck |
| Stream | `zoomcamp/` producers → Redpanda → PyFlink |
| Lake (silver + orchestration) | Airflow-driven bronze/silver on MinIO beyond manual upload |
| Warehouse (cloud) | MotherDuck `transitpulse` (local DuckDB slice exists) |
| Cloud (Phase 1) | `zoomcamp/terraform/` → Oracle Always Free VM + lean graded Compose |
| API (extras) | `extras/api/` FastAPI `/health` + `/metrics/*` + `/ops/freshness` |
| UI (extras) | `extras/nextjs/` — browser → FastAPI only |
| ML (extras) | `extras/ml/` — Phase 2+ |
| CI | `.github/workflows/ci.yml` |
| StopPoint subset | Curated stop list for arrivals stream / API snapshots |

## Phase tracker

| Phase | Intent | Status |
|-------|--------|--------|
| 0 | Spec, LAYOUT, docs, agent context | Shipped |
| 1 Zoomcamp | Vertical slice in `zoomcamp/` + Terraform free VM (`zoomcamp/terraform/`) | In progress (MinIO bronze upload + local dbt/dashboard) |
| 1 extras API | FastAPI metrics in `extras/api/` on gold marts | Not started |
| 1 extras UI | Next.js in `extras/nextjs/` calling FastAPI only | Not started |
| 2 | Feature marts, XGBoost, MLflow, batch scores (`extras/ml/`) | Not started |
| 3 | `POST /predict` + Next.js predict view | Not started |

## Notes for agents

- **Canonical gold** for Streamlit, FastAPI, and (via API) Next.js = MotherDuck dbt marts, not MinIO “gold files.”
- Prefer one thin working slice in **`zoomcamp/`** over standing up every container.
- `cd zoomcamp && docker compose up -d` starts **MinIO only** today. Do not add FastAPI or Next.js to that Compose file.
- Phase 1 Terraform: laptop slice **before** `terraform apply`. Lean VM only — see [cloud-and-free-demo.md](./cloud-and-free-demo.md).
- Next.js must not embed `MOTHERDUCK_TOKEN`.
