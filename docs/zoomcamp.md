# Zoomcamp course project

Course project for DataTalksClub Data Engineering Zoomcamp. Full spec: [PROJECT.md](../PROJECT.md). Graded code: **`zoomcamp/`**. Portfolio-only: **`extras/`** ([LAYOUT.md](../LAYOUT.md)).

Sources: TfL Open Data + Open-Meteo.

## What to ship

- **Problem** — who/what/why in [PROJECT.md](../PROJECT.md) §1 and the README.
- **Cloud** — Terraform + free VM (`zoomcamp/terraform/`) — [cloud-and-free-demo.md](./cloud-and-free-demo.md).
- **Ingestion** — one working **batch or stream** path plus a README explainer. Stream: Redpanda + PyFlink. Batch: Airflow + **dlt** → MinIO/MD. Both in-repo is fine; graders need one path that runs.
- **Warehouse** — MotherDuck + **written** partition/cluster explanation next to models ([data-model.md](./data-model.md)).
- **Transforms** — dbt (+ PySpark for heavy lake jobs).
- **Dashboard** — **2 tiles**: categorical (by line) + temporal (by date).
- **Reproducibility** — clean clone, `cd zoomcamp`, `.env.example`, working Compose, `sample_data/`.

## Reproducibility bar

- Dedicated GitHub repo.
- Secrets only in `.env` / GitHub Actions secrets — never committed or printed in CI logs.
- From **`zoomcamp/`**, `docker compose up` starts the declared graded services (when Compose exists). FastAPI is not in that file.
- `zoomcamp/Makefile`: `up` / `test` / `dbt-build` (when added). An `api` target is extras-only.
- Test from a **clean clone** before submit.

## Scope trap

Zoomcamp does **not** require Airflow + Spark + Flink + Redpanda + MinIO + MotherDuck + dbt + dashboard + FastAPI + Next.js all running 24/7. FastAPI and Next.js are **extras**. A half-running architecture zoo fails reproducibility. Ship a **vertical slice** in `zoomcamp/`, screenshot the rest if needed, and keep [status.md](./status.md) honest.

## Cloud vs demo URL

- **Phase 1 Zoomcamp cloud:** Terraform + free VM (lean Compose). See [phases.md](./phases.md).
- Recruiter/peer **clickable dashboard** ≈ Streamlit Community Cloud + MotherDuck (heavy jobs can stay local).
