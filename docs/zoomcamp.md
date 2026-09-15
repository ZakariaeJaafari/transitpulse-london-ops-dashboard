# Zoomcamp rubric

Course project for DataTalksClub Data Engineering Zoomcamp. Full spec: [PROJECT.md](../PROJECT.md). Graded code: **`zoomcamp/`**. Portfolio-only: **`extras/`** ([LAYOUT.md](../LAYOUT.md)).

Sources: TfL Open Data + Open-Meteo.

## Self-check

| Criterion | Max | How this project aims for it |
|-----------|-----|------------------------------|
| Problem description | 4 | [PROJECT.md](../PROJECT.md) §1 + README problem |
| Cloud | 0 or 4 | **Phase 1:** Terraform + free VM (`zoomcamp/terraform/`) for **4** — [cloud-and-free-demo.md](./cloud-and-free-demo.md) |
| Ingestion (batch **or** stream) | 4 | Stream: Redpanda + PyFlink; batch: Airflow + **dlt** → MinIO/MD. Implement both; graders need one working path + a clear README explainer |
| Data warehouse | 4 | MotherDuck + **written** partition/cluster explanation next to models ([data-model.md](./data-model.md)) |
| Transformations | 4 | dbt (+ PySpark for heavy lake jobs) |
| Dashboard | 4 | **2 tiles**: categorical (by line) + temporal (by date) |
| Reproducibility | 4 | Clean clone, `cd zoomcamp`, `.env.example`, working Compose, `sample_data/` |

Peer reviews: evaluate **3** peers for bonus points.

## Reproducibility bar

- Dedicated GitHub repo.
- Secrets only in `.env` / GitHub Actions secrets — never committed or printed in CI logs.
- From **`zoomcamp/`**, `docker compose up` starts the declared graded services (when Compose exists). FastAPI is not in that file.
- `zoomcamp/Makefile`: `up` / `test` / `dbt-build` (when added). An `api` target is extras-only.
- Test from a **clean clone** before submit.

## Scope trap

Zoomcamp does **not** require Airflow + Spark + Flink + Redpanda + MinIO + MotherDuck + dbt + dashboard + FastAPI + Next.js all running 24/7. FastAPI and Next.js are **extras**. A half-running architecture zoo fails reproducibility. Ship a **vertical slice** in `zoomcamp/`, screenshot the rest if needed, and keep [status.md](./status.md) honest.

## Cloud vs demo URL

- Cloud **4/4** is a **Phase 1 Zoomcamp** deliverable: Terraform + free VM (lean Compose). See [phases.md](./phases.md).
- Recruiter/peer **clickable dashboard** ≈ Streamlit Community Cloud + MotherDuck (heavy jobs can stay local).
