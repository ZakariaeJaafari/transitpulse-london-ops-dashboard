# Implementation status

**As of:** 2026-11-09

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
| Local lake (MinIO) | `zoomcamp/docker-compose.yml` (MinIO + Airflow + Redpanda), `zoomcamp/.env.example`, `zoomcamp/lake/upload_bronze.py` → bronze prefixes; `zoomcamp/lake/line_status_to_silver.py` → typed silver Parquet (host or `transit_batch` task `write_line_status_silver`); `zoomcamp/lake/arrivals_by_line_to_silver.py` → silver JSON lines from the newest Flink `part-*` (host boto3; Flink S3 plugin not used); `zoomcamp/lake/arrival_updates_to_silver.py` → same pattern for event-level predictions under `silver/arrival_updates/`; `zoomcamp/lake/arrivals_by_line_to_parquet.py` → typed Parquet beside that JSON (DuckDB; JSON object kept). Gold stays MotherDuck marts. |
| Redpanda (laptop) | `zoomcamp/docker-compose.yml` service `redpanda` — Kafka external listener `127.0.0.1:19092` (host clients; not `localhost` on this Windows laptop). Topic `tfl.arrivals` (1p/RF1) via `rpk topic create`. Sample publish/consume: `uv run python ingest/publish_sample_arrivals.py`, `uv run python ingest/consume_sample_arrivals.py` (`confluent-kafka`); or verify with `rpk topic consume tfl.arrivals -o start -n 2`. `poll_live_arrivals.py` GETs the current TfL Arrivals board for the four StopPoints and appends it to `tfl.arrivals` (`flush()`); it does not overwrite `sample_data/` or the marts. `consume_new_arrivals.py` uses group `transitpulse-arrivals-live` and commits after a successful read (2026-11-05: the new poll was `obs_ts` `2026-11-05T19:13:54Z`, 190 records, committed through offset 599). `count_arrival_obs_ts.py` counts the log by `obs_ts` and does not commit. VM Compose unchanged; Flink not in Compose yet. |
| Orchestration (local) | `zoomcamp/dags/tube_status.py`, `weather_hourly.py`, `arrivals_by_line.py`, and `transit_batch.py`. DAG `arrivals_by_line`: `load_arrival_updates` → `build_arrivals_mart` (`dbt build --select +arrivals_by_line +arrival_updates arrivals_by_line_matches_updates --no-partial-parse`; 13 data tests). Run `manual__2026-11-04T16:21:44+00:00` succeeded (start 16:21:45 UTC, end 16:22:08 UTC). Prior run `manual__2026-11-04T15:55:31+00:00` used `dbt run` only (`PASS=4`). No Flink/Redpanda in this DAG. `transit_batch`: `load_tube_bronze` → `write_line_status_silver` → `load_weather_bronze` → `build_line_status_mart` (`--select +line_status_snapshot --no-partial-parse`) → `build_weather_mart` (`--select +weather_hourly --no-partial-parse`). `weather_hourly` DAG run `manual__2026-11-01T16:57:47+00:00` succeeded; dbt built `staging_weather_hourly` then `weather_hourly` (16:58:05 UTC, log path `/opt/transitpulse`). `transit_batch` run `manual__2026-11-01T17:00:43+00:00` succeeded (start 17:00:44 UTC, end 17:01:25 UTC); Tube dbt task built `staging_line_status` then `line_status_snapshot`, weather dbt task built `staging_weather_hourly` then `weather_hourly`. Earlier laptop runs: `manual__2026-11-01T16:42:06+00:00` (Tube staging mart only), `manual__2026-11-01T16:20:55+00:00` (with silver task), `manual__2026-11-01T15:42:33+00:00` (before silver). Compose Airflow `:8080`; on this Windows host use **http://127.0.0.1:8080**. Container `MINIO_ENDPOINT=http://minio:9000`. |
| Python env | `zoomcamp/pyproject.toml` + `uv.lock` (uv) |
| Batch ingest (dlt) | `zoomcamp/ingest/tfl_line_status_dlt.py`, `weather_hourly_dlt.py`, `arrivals_by_line_dlt.py`, `arrival_updates_dlt.py`, `fetch_raw.py` — dlt → MotherDuck `transitpulse.bronze.*` (`motherduck_token` / `MOTHERDUCK_TOKEN`). Airflow `arrivals_by_line` DAG uses `arrival_updates_dlt.py` (JSON → Parquet → `bronze.arrival_updates`). `arrivals_by_line_dlt.py` remains for manual Flink-summary Parquet loads into `bronze.arrivals_by_line`. |
| Warehouse (local dev) | DuckDB file `zoomcamp/data/transitpulse.duckdb` for dbt **dev** target only (`marts.*` views); not the live bronze path after Lesson 24 |
| Warehouse (MotherDuck) | `transitpulse`: `bronze.*` from dlt. dbt **motherduck** target builds `marts.staging_line_status` → `marts.line_status_snapshot`, `marts.staging_weather_hourly` → `marts.weather_hourly`, `marts.staging_arrivals_by_line` → `marts.arrivals_by_line`, and `marts.staging_arrival_updates` → `marts.arrival_updates` (staging `DISTINCT` collapses duplicate event publish). Thin marts select from staging. `make dbt-build` includes `+arrivals_by_line` and `+arrival_updates` with the two graded marts. Earlier laptop run (2026-11-01): PASS=4 run / PASS=6 test before arrivals mart. No local attach. On 2026-11-09 `dbt show` still returned `arrivals_by_line` Victoria `n=1` average 65 and Bakerloo `n=1` average 123 (`obs_date` 2026-11-01), and two `arrival_updates` rows at `940GZZLUOXC` (vehicles 235 and 007). The live topic slice is not loaded into these tables. |
| Dashboard (graded) | `zoomcamp/dashboard/` Streamlit — thin `app.py`, `data/` (one loader per mart), `charts/` (one file per tile). **2 graded tiles** (`marts.line_status_snapshot`, `marts.weather_hourly`) plus optional full-width `charts/arrivals.py` (bar chart from `marts.arrivals_by_line`, prediction table from `marts.arrival_updates`; stream sample `obs_date=2026-11-01`). Chart and table calls use `width="stretch"`. A Streamlit restart on 2026-11-09 logged no `use_container_width` warning. The page reads the MotherDuck marts, not `stream/output/arrival_updates_live/` |
| Sample | `zoomcamp/sample_data/` (includes `stoppoints.json` mirroring `ingest/stoppoints.py`) |
| StopPoint subset | `zoomcamp/ingest/stoppoints.py` — four curated naptanIds for future arrivals stream; `tests/test_stoppoints.py` |
| Arrivals proxy (offline) | `zoomcamp/ingest/arrivals_proxy.py` + `sample_data/arrivals_oxford_circus.json`; `tests/test_arrivals_proxy.py` — tube-only, subset stops; `time_to_station_sec` is TfL prediction, not delay |
| Arrivals poll (pure) | `zoomcamp/ingest/tfl_arrivals_producer.py` — `poll_arrivals`, `publish_arrivals(messages, send)` (partition key `stop_id`); `tests/test_tfl_arrivals_producer.py` (injected fetch/send, fixed `obs_ts`) |
| PyFlink | `zoomcamp/stream/print_arrivals_sample.py` — in-memory tuples. `print_arrivals_topic.py` — bounded DataStream read. `print_arrivals_by_line.py` / `write_arrivals_by_line.py` — batch SQL aggregate by `line_id`, optional filesystem JSON sink. `write_arrival_updates.py` — bounded Kafka read from `earliest-offset` (group `transitpulse-flink-arrival-updates`), no aggregate; one JSON object per prediction under `stream/output/arrival_updates/`. `write_live_arrival_updates.py` — separate bounded job (group `transitpulse-flink-live-slice`) from `partition:0,offset:409` through the latest offset into `stream/output/arrival_updates_live/` (not uploaded to MinIO). `summarize_live_slice.py` counted that file on 2026-11-05: 190 predictions, 11 lines, `obs_ts` `2026-11-05T19:13:54Z`. Host `lake/arrivals_by_line_to_silver.py` uploads the newest `part-*` to MinIO `silver/arrivals_by_line/obs_date=2026-11-01/arrivals_by_line.json`; `lake/arrival_updates_to_silver.py` uploads the newest event `part-*` to `silver/arrival_updates/obs_date=2026-11-01/arrival_updates.json`; `lake/arrivals_by_line_to_parquet.py` writes typed Parquet beside that JSON; `ingest/arrivals_by_line_dlt.py` loads that Parquet into MotherDuck `bronze.arrivals_by_line`. `ingest/arrival_updates_dlt.py` writes event Parquet from silver JSON and loads `bronze.arrival_updates` (replace). Same Kafka connector JAR; `apache-flink` 2.2.x, Java 17+. Flink does not write S3 itself. |
| CI | `.github/workflows/ci.yml` — `uv sync` + `pytest` in `zoomcamp/`; `dbt parse --target dev` in `transitPule_dbt/`; `docker compose config --quiet` with placeholder MinIO creds (no `.env`); `apache/airflow:2.10.4-python3.12` + `DagBag('dags')` mount check (no webserver) on push/PR |
| Makefile | `zoomcamp/Makefile` — `up`, `test`, `dbt-build` (no extras API target). On Windows, `dbt-build` uses `bash.exe` and loads `.env` from the Makefile directory. `dbt-build`: one `dbt build` with `+line_status_snapshot +weather_hourly +arrivals_by_line +arrival_updates arrivals_by_line_matches_updates --no-partial-parse` (8 models + 19 data tests). |
| Cloud IaC (Phase 1) | `zoomcamp/terraform/` applied in **austriaeast** zone 1: Ubuntu VM `transitpulse-zoomcamp` (`Standard_B2s_v2`), VNet, public IP, NSG on subnet `transitpulse-public` (SSH :22 from the operator CIDR only). Log: [azure-vm-move.md](./azure-vm-move.md). |
| Cloud Compose (Phase 1) | On that VM, MinIO and Airflow are healthy. `tube_status` run `manual__2026-10-30T19:09:45+00:00` and `weather_hourly` run `manual__2026-10-30T19:09:48+00:00` succeeded (2026-10-30): MotherDuck `transitpulse.bronze` and the two marts. UIs from the laptop use SSH tunnels: Airflow `localhost:8081`, MinIO console `localhost:19001`. On 2026-10-31 Streamlit ran on the VM host (not a Compose service), tunneled at `localhost:8502`, and rendered both MotherDuck tiles. Log: [azure-vm-move.md](./azure-vm-move.md). VM deallocated after that session. |

## Target (not built yet)

| Area | Target |
|------|--------|
| Local stack (full) | PyFlink as a Compose service; a continuous Flink job (event-time windows and checkpoints). The laptop live poll already appends to `tfl.arrivals` and is not the mart source. |
| Stream | Continuous PyFlink on Redpanda. The laptop bounded jobs and the live poll-to-topic script are shipped; the live file is not loaded into MotherDuck. |
| API (extras) | `extras/api/` FastAPI `/health` + `/metrics/*` + `/ops/freshness` |
| UI (extras) | `extras/nextjs/` — browser → FastAPI only |
| ML (extras) | `extras/ml/` — Phase 2+ |

## Phase tracker

| Phase | Intent | Status |
|-------|--------|--------|
| 0 | Spec, LAYOUT, docs, agent context | Shipped |
| 1 Zoomcamp | Vertical slice in `zoomcamp/` + Terraform free VM (`zoomcamp/terraform/`) | VM slice done (Compose, `tube_status` and `weather_hourly`, Streamlit on the VM, deallocated 2026-10-31). Laptop `transit_batch` run `manual__2026-11-01T17:00:43+00:00` built MinIO silver, both bronze loads, and all four dbt tables (two staging + two marts). Laptop bounded stream path is in place (sample and a live poll on `tfl.arrivals`; Flink files on disk; marts still the 2026-11-01 sample). Continuous Flink and Flink-in-Compose are still open. |
| 1 extras API | FastAPI metrics in `extras/api/` on gold marts | Not started |
| 1 extras UI | Next.js in `extras/nextjs/` calling FastAPI only | Not started |
| 2 | Feature marts, XGBoost, MLflow, batch scores (`extras/ml/`) | Not started |
| 3 | `POST /predict` + Next.js predict view | Not started |

## Notes for agents

- **Canonical gold** for Streamlit, FastAPI, and (via API) Next.js = MotherDuck dbt marts, not MinIO “gold files.”
- Prefer one thin working slice in **`zoomcamp/`** over standing up every container.
- `cd zoomcamp && docker compose up -d --wait` starts **MinIO + Airflow** (container healthcheck on `http://127.0.0.1:8080/health`). Redpanda is separate: `docker compose up -d redpanda`; host Kafka clients use **`127.0.0.1:19092`**, not `localhost` (same IPv6/`wslrelay` issue as Airflow). On this Windows laptop the Airflow browser URL is **http://127.0.0.1:8080**. Upload bronze from the **host** (`uv run python lake/upload_bronze.py` uses `localhost:9000`). DAG tasks use `http://minio:9000`. Do not add FastAPI or Next.js to that Compose file.
- Phase 1 compute is one Azure VM from a **$100 credit** ([ADR 0002](./decisions/0002-azure-credit-vm.md)). Laptop slice **before** `terraform apply`. Lake stays MinIO — do not add Azure Blob. Deallocate the VM when idle. See [cloud-and-free-demo.md](./cloud-and-free-demo.md).
- Next.js must not embed `MOTHERDUCK_TOKEN`.
