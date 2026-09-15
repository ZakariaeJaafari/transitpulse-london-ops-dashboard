# TransitPulse — repo layout (Zoomcamp vs extras)

Peers: **grade / review `zoomcamp/`** first. Everything under `extras/` is optional portfolio work and is **not** required for Zoomcamp points.

```
transitpulse/
  README.md                 # start here — points to zoomcamp/ vs extras/
  PROJECT.md
  LAYOUT.md                 # this file
  AGENTS.md                 # shared instructions for coding agents
  CLAUDE.md                 # Claude Code overlay (@AGENTS.md)
  docs/                     # living wiki — update with implementation
  .cursor/                  # project rules + skills (not mcp.json as canon)
    rules/
    skills/

  zoomcamp/                 # ★ COURSE PROJECT (publish & peer-review this)
    README.md               # how to run the graded pipeline only
    docker-compose.yml      # core services for the course path
    .env.example
    pyproject.toml          # or requirements for core
    airflow/dags/           # orchestration → lake
    dlt/                    # batch/API ingest
    spark/jobs/             # batch processing to lake (as needed)
    flink/jobs/             # stream path (if submitting stream track)
    producers/              # Redpanda producers
    dbt/                    # warehouse transforms + tests
    dashboard/              # Streamlit or Metabase notes — 2 tiles
    sample_data/
    terraform/              # Phase 1: Oracle Always Free VM (Cloud 4/4)

  extras/                   # ★ PORTFOLIO ONLY — say so in README
    README.md               # “Not required for Zoomcamp grading”
    api/                    # FastAPI — backend for nextjs/
    ml/                     # training, MLflow, notebooks
    ci-extra/               # optional stricter workflows
    nextjs/                 # product UI — talks only to extras/api/
```

## What goes in `zoomcamp/` (rubric)

| Rubric item | Lives in |
|-------------|----------|
| Problem description | root `PROJECT.md` + `zoomcamp/README.md` |
| Ingest batch/stream → **data lake (MinIO)** | `zoomcamp/dlt/`, `zoomcamp/airflow/`, `zoomcamp/spark/`, `zoomcamp/flink/`, `zoomcamp/producers/` |
| Lake → **warehouse** | `zoomcamp/` load tasks + MotherDuck |
| **dbt** transforms | `zoomcamp/dbt/` |
| **Dashboard** ≥2 tiles | `zoomcamp/dashboard/` (reads marts **directly**; do not call `extras/api/`) |
| Cloud + IaC (Phase 1, Cloud 4/4) | `zoomcamp/terraform/` + [docs/cloud-and-free-demo.md](./docs/cloud-and-free-demo.md) |
| Reproducibility | `zoomcamp/README.md`, `zoomcamp/docker-compose.yml`, `zoomcamp/.env.example` |

## What goes in `extras/` (tell peers)

- FastAPI product API (`extras/api/`) — **backend for Next.js**; not in `zoomcamp/docker-compose.yml`
- Next.js product UI (`extras/nextjs/`) — **only** calls FastAPI; never the Zoomcamp dashboard
- ML / `/predict` / MLflow (`extras/ml/`)
- Nice-to-have CI  

Root README should say clearly:

> **Zoomcamp submission:** run and review `zoomcamp/`.  
> **`extras/`** is additional portfolio work and is not part of the graded scope.
