# TransitPulse — Project Specification

**Course:** DataTalksClub Data Engineering Zoomcamp — Course Project  
**Author:** Zakariae Jaafari  
**Repo title (recommended):** `transitpulse-london-ops-dashboard`  
**Repo split:** graded work in `zoomcamp/`; portfolio add-ons in `extras/` — see [LAYOUT.md](./LAYOUT.md).  
**Constraint:** Local-first lake (no paid GCS, Azure Blob, or AWS S3). Phase 1 compute is one Azure VM paid from a $100 credit, created with Terraform. Warehouse target remains MotherDuck.  
**Sources:** TfL Open Data + Open-Meteo only.

---

## 1. Problem description

### Who has the problem?

Transit operators, city ops analysts, and product teams that build travel tools need a trustworthy view of **service reliability** and how it relates to **weather** — without manually stitching APIs and spreadsheets.

### What is wrong today?

Live arrival predictions, static network structure, and weather live in separate systems. Numbers disagree, freshness is unclear, and there is no reproducible path from raw feeds → lake → warehouse → dashboard.

### What TransitPulse does

TransitPulse is an **end-to-end mobility data platform** for Greater London that:

1. Ingests **static network / timetable-related open data** and **live TfL arrivals / line status** (stream + batch), with **dlt** for structured batch/API loads into the lake or warehouse.
2. Lands raw and cleaned data in a **MinIO data lake** (medallion: bronze → silver → gold files).
3. Loads curated tables into a **MotherDuck** warehouse (free cloud DuckDB) — or Postgres/DuckDB if fully on-prem.
4. Transforms with **dbt** (and **PySpark** for heavy lake batch) into dashboard-ready marts.
5. Serves a **dashboard with ≥2 tiles** (categorical + temporal) in `zoomcamp/dashboard/` (Streamlit preferred).
6. **Extras (not graded):** **FastAPI** in `extras/api/` as the product backend (gold metrics, later `/predict`).
7. **Extras (not graded):** **Next.js** in `extras/nextjs/` as the product UI — **only** calls FastAPI. Streamlit in `zoomcamp/dashboard/` remains the graded dashboard.
8. **Extras (not graded):** **ML delay prediction** (MLflow) on the same gold contracts.
9. Uses **GitHub Actions CI** so every change is checked before merge (graded jobs validate `zoomcamp/`).

### Questions the dashboard answers

- Which Tube **lines** (categorical) show the worst delay / disruption share in the selected window?
- How does **reliability / delay severity** change **over days** (temporal), and does bad weather coincide?

### Success criteria (product)

**Zoomcamp (grade `zoomcamp/`):**

- Peer can clone the repo, `cd zoomcamp`, `docker compose up`, run one Airflow DAG, build dbt, open the dashboard with 2 clear tiles.
- README explains lake vs warehouse, batch vs stream, and partition/cluster choices.
- GitHub Actions CI is green on `main` for the graded path (tests + DAG/dbt/`zoomcamp` compose checks).
- `zoomcamp/terraform/` provisions a free VM; lean graded Compose runs there (laptop slice first).

**Extras (not required for peer grading):**

- FastAPI `/health` and metrics routes in `extras/api/` respond against gold tables.
- Next.js in `extras/nextjs/` calls **only** that API (no warehouse token in the browser).
- Phase 2–3 path (ML + `/predict` + Next.js predict view) is documented even if implemented after Zoomcamp submit.

---

## 2. Dataset selection

| Source | Role | Mode | Access | Official URLs |
|--------|------|------|--------|----------------|
| **TfL Unified API** — StopPoint arrivals | Live predicted arrivals at stops | **Stream** (poll → Redpanda) | Free registration: `app_id` + `app_key` | Portal/docs: https://tfl.gov.uk/info-for/open-data-users/api-documentation · API root: https://api.tfl.gov.uk/ · Swagger: https://api.tfl.gov.uk/swagger/ui/index.html · Example: `GET https://api.tfl.gov.uk/StopPoint/{id}/Arrivals?app_key=...` |
| **TfL Unified API** — Line status | Line disruption / severity | Stream or frequent batch | Same keys | `GET https://api.tfl.gov.uk/Line/Mode/tube/Status?app_key=...` |
| **TfL Journey Planner timetables** | Multimodal working timetables (Tube, bus, DLR, …), weekly | **Batch** | Open ZIP (live set) | https://tfl.gov.uk/journey-planner-timetables.zip · Open-data index: https://tfl.gov.uk/info-for/open-data-users/our-open-data |
| **TfL station topology (GTFS)** | Step-free / station topology in GTFS form | Batch | Open | https://api.tfl.gov.uk/stationdata/tfl-stationdata-gtfs.zip · Guide: https://tfl.gov.uk/cdn/static/cms/documents/step-free-access-and-toilet-data-guide.pdf |
| **Optional GTFS (Interline/Transitland conversion)** | Standard GTFS for Spark/dbt demos if JP ZIP format is awkward | Batch | Attribution required | Feed page: https://www.transit.land/feeds/f-transport~for~london · Current ZIP (check token policy): http://gtfs-source-feeds.transit.land/uk-tfl.zip |
| **Open-Meteo Forecast API** | Current + recent weather for London | Stream poll / batch | No key (non-commercial) | Docs: https://open-meteo.com/en/docs · Example: `https://api.open-meteo.com/v1/forecast?latitude=51.5074&longitude=-0.1278&hourly=temperature_2m,precipitation,wind_speed_10m&timezone=Europe%2FLondon` |
| **Open-Meteo Historical / Archive API** | Past weather for joins | Batch | No key | Docs: https://open-meteo.com/en/docs/historical-weather-api · Endpoint: `https://archive-api.open-meteo.com/v1/archive?...` |

### Terms / attribution (put in README footer)

- TfL: https://tfl.gov.uk/corporate/terms-and-conditions/transport-data-service · Attribution like **“Data provided by Transport for London”** / Powered by TfL Open Data where required. Not an official TfL product.
- Open-Meteo: follow https://open-meteo.com/en/terms (free non-commercial; attribution appreciated).

### Why these sources (peer-review friendly)

- Industry-recognizable **transit + weather** join (London ops, not a generic tutorial dataset).
- Mix of **batch files** and **live HTTP APIs** → justifies Airflow + **dlt** + Spark **and** Redpanda + Flink.

---

## 3. Pipeline decision: batch **and** stream

Zoomcamp expects **either** batch **or** stream for ingest. TransitPulse **implements both**; README declares:

- **Primary graded stream path:** producer → **Redpanda** → **PyFlink** → MinIO bronze/silver.
- **Also implemented batch path:** **Airflow** → **dlt** (TfL/Open-Meteo → MinIO / MotherDuck) → optional **PySpark** → **dbt**.

### Batch (Airflow + dlt + PySpark)

**dlt** ([data load tool](https://dlthub.com)) = Python ingest framework (sources → destinations, incremental, schema evolution).  
**dbt** = transforms *inside* the warehouse. Do not confuse the two.

1. `ingest_tfl_static_dlt` — Airflow triggers **dlt** pipeline: Journey Planner ZIP / station GTFS / file sources → MinIO bronze (and/or DuckDB/MotherDuck landing).  
2. `ingest_open_meteo_dlt` — **dlt** pulls Open-Meteo archive/forecast HTTP → bronze/staging (incremental by date).  
3. `ingest_tfl_api_snapshot_dlt` — optional periodic **dlt** snapshot of selected Unified API resources into bronze (complements stream).  
4. `spark_parse_static_network` — PySpark for heavy parse/normalize/joins on large extracts → silver Parquet on MinIO.  
5. `load_motherduck` — promote curated silver into MD (if not already loaded by dlt).  
6. `dbt_build` — dbt run/test on MotherDuck.  
7. Sensors on freshness / Flink lag (optional).

### Stream (Redpanda + PyFlink)

1. `tfl_arrivals_producer` — poll selected StopPoint IDs + line status → Redpanda topics `tfl.arrivals`, `tfl.line_status`
2. `weather_poll_producer` — Open-Meteo current → `weather.london`
3. PyFlink job — windows (e.g. 5-min delay proxies / status severity), sink Parquet to MinIO and/or upsert staging tables

---

## 4. Architecture

```
TfL timetables / GTFS / Open-Meteo archive ──Airflow──► dlt ──► MinIO bronze / MD staging
                                              └──────────► PySpark ──► MinIO silver
TfL Unified API (arrivals, line status) ──► producer ──► Redpanda ──► PyFlink ──► MinIO / staging
Open-Meteo current ──► poller / dlt snapshot ──────────────────────────────────────────┘
                                                              ▼
                                                   MotherDuck ◄── dbt (staging → marts)
                                                              ▼
                                              Metabase / Streamlit (2 tiles)
```

| Layer | Technology | Notes for Zoomcamp “Technologies” section |
|-------|------------|-------------------------------------------|
| IaC (Phase 1) | Terraform | `zoomcamp/terraform/` → Azure VM ($100 credit) + lean Compose. Lake stays MinIO. |
| Orchestration | Apache Airflow | DAGs, UI graph, retries; runs dlt pipelines as tasks |
| Ingest (batch APIs/files) | **dlt** | Load TfL + Open-Meteo into MinIO/MotherDuck; incremental + schema; https://dlthub.com |
| Data lake | **MinIO** | S3-compatible; explains “what MinIO is” in README |
| Stream bus | **Redpanda** | Kafka API compatible |
| Stream processing | **PyFlink** | Windows / sinks |
| Batch processing | **PySpark** | Parse + joins + reconcile |
| Data warehouse | **MotherDuck** | Free DuckDB cloud; or local Postgres |
| Transforms | **dbt** | Models, tests, docs |
| Packaging | **uv** | Python envs |
| Dashboard | Metabase or Streamlit | ≥2 tiles |
| Metrics & ML API | **FastAPI** | Phase 1 metrics; Phase 3 `/predict` |
| ML tracking | **MLflow** | Phase 2+ experiments + registry (artifacts on MinIO) |
| CI/CD | **GitHub Actions** | pytest, Airflow DAG parse, dbt parse, compose config |

### Medallion

- **Bronze:** raw API JSON, untouched ZIPs  
- **Silver:** parsed tables, typed columns, conformed keys (`line_id`, `stop_id`, `obs_ts`)  
- **Gold:** dbt marts for dashboard  

---

## 5. Data warehouse design (partitioned + clustered + explanation)

**Target:** MotherDuck database `transitpulse` (or Postgres schemas `raw` / `staging` / `marts`).

### Example marts

| Table | Grain | Partition / cluster strategy | Why (upstream dashboard queries) |
|-------|-------|------------------------------|----------------------------------|
| `marts.fact_arrival_updates` | one prediction/event | Partition by `event_date`; cluster/sort by `line_id`, `stop_id` | Temporal tile filters by date; categorical tile groups by line |
| `marts.fact_line_status_hourly` | line × hour | Partition by `status_date`; cluster by `line_id` | Line disruption over time |
| `marts.dim_line` / `dim_stop` | entity | No partition; small dims | Joins |
| `marts.fact_weather_hourly` | London × hour | Partition by `obs_date` | Weather overlay / correlation |
| `marts.mart_line_reliability_daily` | line × day | Partition by month; cluster by `line_id` | Categorical bar chart |
| `ops.source_freshness` | source × check_ts | — | Ops / reproducibility story |

> MotherDuck/DuckDB: use **partition columns** in table design + intentional `ORDER BY` / indexes where available; if using Postgres, use native `PARTITION BY RANGE (event_date)` and `CLUSTER` / indexes. **Document the choice in README** — graders look for the *explanation*.

---

## 6. Transformations

- **PySpark:** bronze→silver for large timetable/GTFS-like extracts; nightly reconcile of stream vs static.  
- **dbt on MotherDuck:** `staging_*` → `int_*` → `marts_*`; tests `not_null`, `unique`, relationships; docs generated.  
- Airflow triggers `dbt build` after loads.

---

## 7. Dashboard (2 tiles)

**Tool:** Metabase (Docker, talks to MotherDuck) **or** Streamlit (Zoomcamp-familiar).

| Tile | Type | Query idea | Title example |
|------|------|------------|---------------|
| 1 | **Categorical** | Avg disruption severity or % delayed predictions by `line_name` | “Share of disrupted intervals by Tube line (last 7 days)” |
| 2 | **Temporal** | Daily reliability or mean delay proxy by `event_date` | “Daily reliability trend — London Tube (with rainfall overlay optional)” |

Requirements: clear titles, axis labels, data source reference footnote (“TfL Open Data + Open-Meteo”). Screenshots in README for peers.

---

## 8. Folder policy (Zoomcamp vs extras)

- Publish one repo; tell peers: **evaluate `zoomcamp/` only**.  
- Put FastAPI, Next.js, ML under **`extras/`** with a short `extras/README.md` stating they are out of Zoomcamp scope. Next.js talks only to FastAPI.  
- Keep Terraform under `zoomcamp/terraform/`.  
- Details: [LAYOUT.md](./LAYOUT.md).

---

## 9. Reproducibility requirements

- New dedicated GitHub repo (not nested in unrelated monorepo junk).
- `zoomcamp/.env.example` with `TFL_APP_KEY`, `MOTHERDUCK_TOKEN`, MinIO keys — **no secrets committed**.
- From **`zoomcamp/`**, `docker compose up` starts the graded stack: Airflow, MinIO, Redpanda, Flink (if used), Streamlit/Metabase. **Not** FastAPI.
- Sample StopPoint IDs + small sample bronze data in `zoomcamp/sample_data/` for offline demo.
- `zoomcamp/Makefile`: `up` / `test` / `dbt-build`. Extra `api` target belongs under `extras/` if added.
- **GitHub Actions** required for portfolio quality (see §11); compose-validate the **`zoomcamp/`** file.
- Test from a **clean clone** before submit. Tell peers: evaluate `zoomcamp/` only.

---

## 10. FastAPI serving (extras backend — not Zoomcamp-graded)

Code lives in **`extras/api/`**. Do not add it to the graded `zoomcamp/docker-compose.yml`. The Zoomcamp dashboard must query gold marts directly, not `extras/api`.

**Next.js** in `extras/nextjs/` is the product UI for this API (browser → FastAPI → marts). See [docs/phases.md](./docs/phases.md) and [docs/decisions/0001-extras-nextjs-fastapi.md](./docs/decisions/0001-extras-nextjs-fastapi.md).

### Phase 1 extras API — metrics (portfolio; optional beside Streamlit)

| Method | Path | Returns |
|--------|------|---------|
| GET | `/health` | `{ "status": "ok" }` |
| GET | `/metrics/lines` | Per-line reliability / disruption share (same grain as the categorical tile) |
| GET | `/metrics/reliability/daily` | Time series (same grain as the temporal tile) |
| GET | `/ops/freshness` | Last success timestamps per source |
| GET | `/docs` | OpenAPI (automatic) |

Implementation notes:
- Read **gold** MotherDuck tables (or a thin cache), not bronze.
- Timeouts + clear 503 if warehouse unreachable.
- No TfL keys in responses; attribution string in a `/meta` or response header.

### Phase 3 — model serving

| Method | Path | Returns |
|--------|------|---------|
| POST | `/predict` | Delay risk / ETA band for `{ line_id, stop_id, ts, weather_features… }` |
| GET | `/models/active` | Active MLflow model name + version |

Confirm before any public deploy that prompts/PII are not logged. Prefer batch scoring DAG + online `/predict` on the same feature contract.

---

## 11. GitHub Actions CI/CD

File: `.github/workflows/ci.yml`  
Triggers: `push` + `pull_request` to `main`.

### Jobs (minimum)

1. **test** — `uv sync` → `pytest` (producers, pure Spark helpers if any). FastAPI tests only if `extras/api/` is in the job matrix.
2. **airflow-parse** — import DAG bag under `zoomcamp/airflow/` / `airflow dags list`.
3. **dbt-parse** — `dbt deps` + `dbt parse` in `zoomcamp/dbt/` (profile pointing at DuckDB file or MD secret).
4. **compose-validate** — `docker compose -f zoomcamp/docker-compose.yml config -q`.  

### Optional deploy workflow (post-Zoomcamp)

- On tag / `main`: optional extras FastAPI image, `terraform apply` from `zoomcamp/terraform/`, or SSH `docker compose pull && up -d` in `zoomcamp/`.  
- Store `TFL_APP_KEY`, `MOTHERDUCK_TOKEN` only in GitHub **Secrets**.  
- Never print secrets in Action logs.

This is **optional for the course project** but strongly recommended for the “senior SWE + data” hiring story.

---

## 12. ML / DL roadmap (same platform — from earlier architecture)

### Phase 1 — Data platform (Zoomcamp submit)
Ship `zoomcamp/`: lake → warehouse → dbt → 2-tile **Streamlit** dashboard + CI, then **Terraform → free VM** (`zoomcamp/terraform/`, lean Compose). Laptop slice **before** `terraform apply`.

Optional in parallel (`extras/`): FastAPI metrics, then Next.js on that API — **not required for the course project**. Full order: [docs/phases.md](./docs/phases.md).

### Phase 2 — Machine learning
- Feature mart: recent delay windows, line status severity, weather (precip, wind, temp).  
- Train baseline **XGBoost** / sklearn; honest train/validation split by time.  
- **MLflow** tracking; artifacts on MinIO; Airflow `train_delay_model` + `batch_score` DAGs.  
- Write predictions back to MotherDuck for dashboard overlay.

### Phase 3 — Deploy models + product UI
- FastAPI `/predict` with versioned model; Next.js predict view calling that route.
- Shadow vs active, rollback note in ADR.
- Optional **PyTorch** sequence model only after classical ML is solid.
- Load/contract tests; monitor null rates / prediction drift simply (table + alert).  

**Interview line:** “I designed a mobility data product; ML and model serving plug into the same contracts — not a one-off Kaggle notebook.”

---

## 13. Going the extra mile checklist

- [ ] Unit tests + `zoomcamp/Makefile`
- [ ] GitHub Actions CI green (`zoomcamp/` compose/DAG/dbt)
- [ ] FastAPI in **`extras/api/`** with OpenAPI docs (portfolio only)
- [ ] Next.js in **`extras/nextjs/`** talking only to FastAPI (portfolio only)
- [ ] ADRs for batch/stream, **dlt vs hand-rolled ingest**, MinIO, MotherDuck, ML (`docs/decisions/`; Next.js+FastAPI is [0001](./docs/decisions/0001-extras-nextjs-fastapi.md))
- [ ] Phase 2 MLflow delay model in `extras/ml/` (after Zoomcamp if needed)  

---

## 14. Plagiarism note

Original TransitPulse design for this Zoomcamp iteration. Do not copy other course projects or prior bootcamp repos.

---

*Living spec — align README with code as you implement.*
