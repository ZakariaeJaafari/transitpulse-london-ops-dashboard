---
name: zoomcamp-rubric
description: Maps TransitPulse work to DataTalksClub DE Zoomcamp 4-point criteria (problem, cloud, ingest, warehouse, transforms, dashboard, reproducibility). Use when the user mentions Zoomcamp, submit, peer review, rubric, scoring, Cloud 4/4, or grading.
---

# Zoomcamp rubric

Read [docs/zoomcamp.md](../../../docs/zoomcamp.md) and [PROJECT.md](../../../PROJECT.md). Graded code lives in **`zoomcamp/`** only ([LAYOUT.md](../../../LAYOUT.md)).

## Criteria (max 4 each)

| Criterion | Ship |
|-----------|------|
| Problem | Who/what/why in README + `PROJECT.md` §1 (TfL + weather) |
| Cloud | **4** in Phase 1: Terraform + free VM (`zoomcamp/terraform/` + [docs/cloud-and-free-demo.md](../../../docs/cloud-and-free-demo.md)) |
| Ingest | One working **batch or stream** path + README explainer. Both in-repo is extra, not a substitute for one path that runs |
| Warehouse | MotherDuck + **written** partition/cluster explanation next to dbt models |
| Transforms | dbt (+ Spark for heavy lake jobs) |
| Dashboard | **2 tiles**: categorical (by line) + temporal (by date), titles/labels/TfL footnote |
| Reproducibility | Clean clone, `cd zoomcamp`, `.env.example`, working Compose, `sample_data/` |

## Do not

- Put FastAPI, MLflow, or Next.js in the graded story as if they were required (`extras/` is optional). **Do not require FastAPI or Next.js for 24/24.** Do not replace Streamlit with Next.js for graders.
- Wire `zoomcamp/dashboard/` to `extras/api/` for the submit path (tiles must read marts directly).
- Fail reproducibility with a half-running Airflow+Spark+Flink+Redpanda zoo. Prefer a **vertical slice**.
- Claim Cloud 4 with MotherDuck alone; pair Terraform + free VM if chasing 4.

## Before “ready to submit”

- [ ] `cd zoomcamp` clone path works (`docker compose` + 2 tiles; **no FastAPI required**)
- [ ] Two dashboard tiles visible (screenshot in README)
- [ ] Partition/cluster explained in dbt YAML or README
- [ ] Batch vs stream called out
- [ ] `docs/status.md` matches reality
- [ ] Evaluate 3 peer projects (bonus)

Peer reviewers should be told: grade `zoomcamp/`; ignore `extras/`.
