# Phases

Canonical build order. Status of each row: [status.md](./status.md). Serving surfaces: [serving.md](./serving.md). ADR: [0001-extras-nextjs-fastapi.md](./decisions/0001-extras-nextjs-fastapi.md).

Zoomcamp graders review **`zoomcamp/` only**. Next.js is **never** the graded dashboard.

```mermaid
flowchart LR
  P0[Phase0 spec] --> P1[Phase1 zoomcamp slice plus Terraform VM]
  P1 --> P1e[Phase1 extras FastAPI metrics]
  P1e --> P1ui[Phase1 extras Next.js on API]
  P1 --> P2[Phase2 ML]
  P1e --> P2
  P2 --> P3[Phase3 predict]
  P1ui --> P3
```

## Table

| Phase | Folder | Ship | Not this phase |
|-------|--------|------|----------------|
| **0** | root + `docs/` | Spec, LAYOUT, agent docs | Pipeline code |
| **1 Zoomcamp** | `zoomcamp/` | One ingest path → MinIO → MotherDuck → dbt → **2 Streamlit tiles** + CI, then **Terraform → Azure credit VM** (`zoomcamp/terraform/`) running **lean** graded Compose | FastAPI, Next.js, ML; stuffing every container onto the VM; Azure Blob as the lake |
| **1 extras API** | `extras/api/` | FastAPI reads **gold marts**: `/health`, `/metrics/lines`, `/metrics/reliability/daily`, `/ops/freshness` | Putting API in zoomcamp Compose |
| **1 extras UI** | `extras/nextjs/` | Next.js product UI that **only** talks to FastAPI (same 2 questions as Streamlit, plus ops/freshness) | Querying MotherDuck from the browser; replacing Streamlit for graders |
| **2** | `extras/ml/` | Feature marts, XGBoost/sklearn, MLflow, batch scores | `/predict` required for Zoomcamp |
| **3** | `extras/api/` + `extras/nextjs/` | `POST /predict`, `/models/active`, Next.js predict view, versioning | PyTorch until classical ML is solid |

## Hard rules

- Graders: `zoomcamp/` only. Streamlit in `zoomcamp/dashboard/` stays the Zoomcamp 2-tile dashboard.
- Next.js **must not** hold a MotherDuck token in the client. Path is **browser → FastAPI → marts**.
- FastAPI stays off `zoomcamp/docker-compose.yml` (separate extras compose later). **Do not** put FastAPI or Next.js on the Phase 1 VM as a grading requirement.
- Phase 1 Zoomcamp **includes** Terraform + one Azure VM paid from a $100 credit (`zoomcamp/terraform/`). Sequence: **laptop slice first**, then `terraform apply`, then lean Compose on the VM (Airflow + dlt + MinIO + Streamlit). Deallocate the VM when idle. Spark/Flink on-demand or screenshots. Details: [cloud-and-free-demo.md](./cloud-and-free-demo.md).
- Do not start Phase 1 extras UI before the metrics API exists (even as a stub against gold).
- Phase 2 does not block Zoomcamp submit. Phase 3 `/predict` uses the same feature contract as batch scores.

## What exists today

Phase **0** is shipped. Phase 1 Zoomcamp has run on the laptop and on the Azure VM (Compose, `tube_status` + `weather_hourly`, Streamlit over an SSH tunnel). dbt on MotherDuck uses staging then mart for both tiles (`staging_line_status` / `line_status_snapshot`, `staging_weather_hourly` / `weather_hourly`); Airflow and `make dbt-build` use `--select +line_status_snapshot` and `+weather_hourly` so staging runs first. Laptop `weather_hourly` DAG run `manual__2026-11-01T16:57:47+00:00` and `transit_batch` run `manual__2026-11-01T17:00:43+00:00` succeeded (2026-11-01). The VM is deallocated. The laptop bounded stream path is in place, including a live poll that appends to `tfl.arrivals` without reloading the marts. Group `transitpulse-arrivals-live` was committed through offset 2336 on 2026-11-11. A Flink read from offset 1970 wrote a local file only. A Flink read from offset 2163 wrote 173 local rows at `obs_ts` `2026-11-11T22:31:34Z` and is not loaded. `ingest/append_new_arrivals.py` (group `transitpulse-arrivals-append`) starts at offset 2163 when that group has no commit and appends JSON lines to `stream/output/arrival_updates_appended.jsonl`; it does not commit `transitpulse-arrivals-live`, does not poll TfL, and does not upload. The offset-409 folder has three local part files and is not loaded. Each filesystem writer exits 1 when a part file is already present. `follow_arrivals.py` commits an open batch on Ctrl+C and does not commit when the batch is empty. On 2026-11-10 `dbt show --no-partial-parse` still returned the 2026-11-01 sample. Continuous Flink and Flink-in-Compose are still open. Extras phases are not started. See [status.md](./status.md).
