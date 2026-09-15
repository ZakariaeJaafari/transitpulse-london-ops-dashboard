# AGENTS.md

Shared instructions for coding agents (Cursor, Codex, Claude Code via `CLAUDE.md`, Copilot, etc.). Do not copy this file into rules or skills.

## What this repo is

**TransitPulse** is a Greater London mobility lakehouse: TfL open data + Open-Meteo → MinIO (lake) → MotherDuck (warehouse) → dbt marts → 2-tile dashboard.

- Course: DataTalksClub Data Engineering Zoomcamp (plus a portfolio path).
- Sources: TfL Open Data + Open-Meteo only.
- Constraint: local-first / free-tier only (no paid GCS/Azure/AWS lake).

Spec: [PROJECT.md](./PROJECT.md). Phases: [docs/phases.md](./docs/phases.md). Living wiki: [docs/](./docs/README.md). Folder split: [LAYOUT.md](./LAYOUT.md).

## Where code goes

| Scope | Path | Notes |
|-------|------|--------|
| Graded Zoomcamp | `zoomcamp/` | Ingest, lake, warehouse, dbt, 2-tile dashboard, Terraform free VM |
| Portfolio only | `extras/` | FastAPI (BE) + Next.js (FE) + ML/`/predict` — **not** required for peer grading |
| As-built docs | `docs/` | Update with every implementation change ([protocol](./docs/README.md)) |

Do not put FastAPI, Next.js, or MLflow under `zoomcamp/`. Do not put graded dbt/Streamlit under `extras/`. Next.js **only** calls FastAPI (no warehouse token in the client).

## Phases

Canonical table: [docs/phases.md](./docs/phases.md).

1. **Phase 1 Zoomcamp:** vertical slice in `zoomcamp/` — ingest → lake → warehouse → 2 Streamlit tiles + CI, then **Terraform → free VM** (`zoomcamp/terraform/`, lean Compose). Laptop slice **before** `terraform apply`. FastAPI/Next.js are **not** this slice.
2. **Phase 1 extras API:** FastAPI metrics in `extras/api/` on gold marts.
3. **Phase 1 extras UI:** Next.js in `extras/nextjs/` → FastAPI only.
4. **Phase 2:** ML in `extras/ml/` on the same gold contracts.
5. **Phase 3:** `POST /predict` + Next.js predict view — never add extras to `zoomcamp/docker-compose.yml`.

## Stack (target)

| Layer | Tool |
|-------|------|
| Orchestration | Apache Airflow |
| Batch ingest | **dlt** (not the same as dbt) |
| Lake | MinIO (bronze / silver) |
| Stream | Redpanda + PyFlink |
| Heavy batch | PySpark |
| Warehouse | MotherDuck (`transitpulse`) |
| Transforms | **dbt** (`staging_*` → `int_*` → `marts_*`) |
| Dashboard | **Streamlit** in `zoomcamp/dashboard/` (graded 2 tiles; reads marts, not extras API) |
| Product UI | **Next.js** in `extras/nextjs/` → **FastAPI** in `extras/api/` only |
| Packaging | **uv** (not pip/venv as the default) |
| IaC | Terraform in `zoomcamp/terraform/` → Oracle Always Free VM (**Phase 1**) |

**Canonical gold** for dashboard and API = MotherDuck **dbt marts**. MinIO gold files are optional, not the serving source.

TfL `timeToStation` is a **prediction**, not delay vs timetable. Define a delay **proxy** (or a real GTFS/JP join) before naming metrics “delay.” Poll a **StopPoint subset**; keep `zoomcamp/sample_data/` for offline demo.

## Commands

Working directory for compose, dbt, and Airflow is **`zoomcamp/`**. Extras API is a **separate** service (do not fold it into the graded Compose file).

Until [docs/status.md](./docs/status.md) lists them as shipped, **do not assume** these exist:

- `cd zoomcamp && docker compose up` / `make up`
- `cd zoomcamp && make test` / `make dbt-build`

Python env: `uv sync` (when `zoomcamp/pyproject.toml` exists).

## Secrets and attribution

- Secrets only in `zoomcamp/.env` (from `zoomcamp/.env.example`). Never commit tokens, never print them in CI logs or API responses.
- Typical names: `TFL_APP_KEY`, `MOTHERDUCK_TOKEN`, MinIO keys.
- Credit: **Data provided by Transport for London** / Powered by TfL Open Data. Open-Meteo per their terms. Not an official TfL product.

## Docs protocol

Any change to stack, contracts, run steps, or course deliverables updates the matching file under `docs/` **and** `docs/status.md` in the same change. Graded run steps → `zoomcamp/README.md`. Root README only if the clone blurb changes. Follow `.cursor/skills/update-docs`.

## Pointers

- Rules: `.cursor/rules/`
- Skills: `update-docs`, `zoomcamp-rubric`, `medallion-pipeline`, `data-contracts`
- Phases: [docs/phases.md](./docs/phases.md)
- Cloud demo: [docs/cloud-and-free-demo.md](./docs/cloud-and-free-demo.md)
