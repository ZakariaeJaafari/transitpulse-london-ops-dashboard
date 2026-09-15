# TransitPulse

**London mobility ops data platform** — end-to-end lakehouse for DataTalksClub DE Zoomcamp (and portfolio).

Turn **TfL open data** + **Open-Meteo** weather into a **MinIO data lake**, **MotherDuck** warehouse, **dbt** marts, and a **2-tile dashboard** — with **Airflow**, **dlt** (ingest), **PySpark**, **Redpanda**, and **PyFlink**.

---

## Problem

Ops and product teams need a reproducible answer to:

1. **Which Tube lines** look worst right now / this week? *(categorical)*  
2. **How is reliability changing over time**, and does weather line up? *(temporal)*

Raw TfL feeds and weather APIs do not give that alone. TransitPulse builds the pipeline: **ingest → lake → warehouse → transform → dashboard**.

---

## Documentation

Living wiki (update whenever implementation changes): **[docs/](./docs/README.md)**

| Doc | Purpose |
|-----|---------|
| [docs/status.md](./docs/status.md) | As-built vs target |
| [docs/phases.md](./docs/phases.md) | Build order (Zoomcamp vs extras) |
| [docs/diagrams.md](./docs/diagrams.md) | A→Z architecture diagrams |
| [docs/data-model.md](./docs/data-model.md) | Keys, grains, partition/cluster |
| [docs/pipelines.md](./docs/pipelines.md) | Batch vs stream, MinIO, dlt vs dbt |
| [docs/serving.md](./docs/serving.md) | Streamlit + FastAPI + Next.js |
| [docs/zoomcamp.md](./docs/zoomcamp.md) | Course deliverables |
| [docs/cloud-and-free-demo.md](./docs/cloud-and-free-demo.md) | Terraform VM + public demo |
| [AGENTS.md](./AGENTS.md) / [CLAUDE.md](./CLAUDE.md) | Instructions for coding agents |

Product spec (stable): **[PROJECT.md](./PROJECT.md)**.

---

## Architecture (high level)

Full A→Z action diagrams: **[docs/diagrams.md](./docs/diagrams.md)**


```
Batch:  TfL timetables / station GTFS / Open-Meteo archive + API extracts
          └─ Airflow ─► dlt (load) ─► MinIO bronze / MotherDuck staging
                      └─ PySpark (heavy parse/joins) ─► MinIO silver

Stream: TfL Unified API (arrivals, line status) + Open-Meteo current
          └─ producers ─► Redpanda ─► PyFlink ─► MinIO / staging

Warehouse: MotherDuck (free)  ◄── dbt models & tests
Dashboard: zoomcamp/dashboard/ (2 tiles; reads marts — not extras/api)
Extras:    FastAPI backend + Next.js UI (extras/api, extras/nextjs); ML later (extras/ml)
Note: dlt = ingest/load tool; dbt = transform in the warehouse (different tools).
```

| Piece | Tool | Why |
|-------|------|-----|
| Orchestration | Apache Airflow | Multi-step batch DAGs, UI, retries; later train/score DAGs |
| Ingest (batch APIs / files) | **dlt** ([dlthub](https://dlthub.com)) | Python pipelines: TfL/Open-Meteo → MinIO or MotherDuck; incremental + schema; run inside Airflow |
| Data lake | **MinIO** | Local S3-compatible object store (no GCS/Azure); also MLflow artifacts |
| Stream log | **Redpanda** | Kafka-compatible, Docker-friendly |
| Stream compute | **PyFlink** | Windowed processing on live events |
| Batch compute | **PySpark** | Parse large static extracts, joins, reconcile |
| Warehouse | **MotherDuck** | Free cloud DuckDB SQL warehouse |
| Transforms | **dbt** | Versioned SQL, tests, docs (`zoomcamp/dbt/`) |
| Dashboard | Streamlit (preferred) | Zoomcamp 2 tiles in `zoomcamp/dashboard/` — reads **marts**, not extras API |
| Metrics / ML API | **FastAPI** | **`extras/api/`** — backend for Next.js; not required for peer grading |
| Product UI | **Next.js** | **`extras/nextjs/`** — browser → FastAPI only; never the Zoomcamp dashboard |
| ML tracking | **MLflow** (Phase 2+) | **`extras/ml/`** — experiments + model registry |
| CI/CD | **GitHub Actions** | Lint, tests, `zoomcamp` compose/DAG/dbt checks on every PR |
| Python packaging | **uv** | Fast, modern envs |
| IaC (Phase 1) | Terraform | `zoomcamp/terraform/` Oracle Always Free VM |

**Medallion:** bronze (raw) → silver (cleaned) → gold (dbt marts).

---

## Data sources (URLs)

### A. Transport for London — live API (stream)

| Item | URL |
|------|-----|
| Open data overview | https://tfl.gov.uk/info-for/open-data-users/our-open-data |
| Unified API docs | https://tfl.gov.uk/info-for/open-data-users/api-documentation |
| API root | https://api.tfl.gov.uk/ |
| Swagger UI | https://api.tfl.gov.uk/swagger/ui/index.html |
| Register for `app_id` / `app_key` | Via TfL API portal (see docs above) |
| Example — arrivals | `https://api.tfl.gov.uk/StopPoint/{id}/Arrivals` |
| Example — Tube line status | `https://api.tfl.gov.uk/Line/Mode/tube/Status` |

**Auth:** append `app_key` (and `app_id` if required) as query params. Store in `.env`, never commit.

### B. Transport for London — static / batch files

| Dataset | URL | Cadence |
|---------|-----|---------|
| Journey Planner timetables (ZIP) | https://tfl.gov.uk/journey-planner-timetables.zip | ~weekly |
| Station topology GTFS (ZIP) | https://api.tfl.gov.uk/stationdata/tfl-stationdata-gtfs.zip | as published |
| Station topology detailed (ZIP) | https://api.tfl.gov.uk/stationdata/tfl-stationdata-detailed.zip | as published |
| Step-free data guide (PDF) | https://tfl.gov.uk/cdn/static/cms/documents/step-free-access-and-toilet-data-guide.pdf | — |

Optional standard GTFS (third-party conversion — check license/token):

| Dataset | URL |
|---------|-----|
| Transitland TfL feed page | https://www.transit.land/feeds/f-transport~for~london |
| GTFS ZIP (Interline) | http://gtfs-source-feeds.transit.land/uk-tfl.zip |

### C. Open-Meteo — weather (batch + poll)

| Item | URL |
|------|-----|
| Forecast API docs | https://open-meteo.com/en/docs |
| Historical API docs | https://open-meteo.com/en/docs/historical-weather-api |
| Forecast example (London) | https://api.open-meteo.com/v1/forecast?latitude=51.5074&longitude=-0.1278&hourly=temperature_2m,precipitation,wind_speed_10m&timezone=Europe%2FLondon |
| Archive endpoint | https://archive-api.open-meteo.com/v1/archive |

**London reference coordinates:** lat `51.5074`, lon `-0.1278` (adjust if you pick a specific borough centroid).

### D. Legal / attribution

- TfL terms: https://tfl.gov.uk/corporate/terms-and-conditions/transport-data-service  
- Credit: **Data provided by Transport for London** / Powered by TfL Open Data as required. This is **not** an official TfL app.  
- Open-Meteo terms: https://open-meteo.com/en/terms  

---

## Dashboard tiles (minimum)

1. **Categorical:** disruption / delay share **by Tube line** (last 7 days).  
2. **Temporal:** daily reliability (or mean delay proxy) **over time**.  

Optional third: rainfall vs delay scatter (nice-to-have, not required).

---

## Warehouse partitioning

Dashboard queries filter by **date** and group by **line**. Therefore:

- Fact tables carry `event_date` (or `status_date`) as a **partition key**.  
- Frequent filters/joins on `line_id` / `stop_id` → **cluster / sort / index** those columns.  
- Document this next to the dbt models so peer reviewers see intentional design, not accidental tables.

---

## How to run (target — fill as you implement)

Graded stack lives in **`zoomcamp/`**. Peers: ignore `extras/`.

```bash
git clone <your-repo-url> transitpulse
cd transitpulse/zoomcamp
cp .env.example .env   # add TFL_APP_KEY, MOTHERDUCK_TOKEN, etc.
docker compose up -d
# Airflow UI: http://localhost:8080
# MinIO console: http://localhost:9001
# Streamlit / Metabase: see compose ports
```

Then:

1. Unpause DAGs in Airflow (static ingest → Spark → load → dbt).
2. Confirm Flink job + producers are up (`docker compose ps`) if you are submitting the stream track.
3. `dbt deps && dbt build` (or via Airflow).
4. Open the **2-tile dashboard** in `zoomcamp/dashboard/`.

> Before submission: test from a **clean clone** on another folder/machine. FastAPI is **not** part of this path (`extras/api/`).

---

## Zoomcamp vs extras (folder split)

| Folder | Purpose |
|--------|---------|
| **`zoomcamp/`** | Graded course project only — ingest → MinIO lake → MotherDuck → dbt → 2-tile dashboard + Terraform free VM |
| **`extras/`** | Portfolio only — FastAPI **backend** + Next.js **UI**, ML/`/predict`. **Not required for peer grading.** |

See **[LAYOUT.md](./LAYOUT.md)** for the full tree.

> **Peers:** please review **`zoomcamp/`**. Treat **`extras/`** as optional add-ons.

---

## Project layout (target)

```
transitpulse/
  README.md
  PROJECT.md
  LAYOUT.md
  AGENTS.md
  CLAUDE.md
  docs/                     # living wiki (status, pipelines, data model)
  zoomcamp/                 # ★ Zoomcamp (publish / grade this)
    README.md
    docker-compose.yml
    .env.example
    airflow/dags/
    dlt/
    spark/jobs/
    flink/jobs/
    producers/
    dbt/
    dashboard/              # 2 tiles
    sample_data/
    terraform/              # Phase 1: Oracle Always Free VM
  extras/                   # ★ portfolio only (say so to peers)
    README.md
    api/                    # FastAPI backend for nextjs/
    ml/                     # MLflow, train, /predict later
    nextjs/                 # product UI — FastAPI only; not for Zoomcamp
```


---

## Cloud & free public demo

Step-by-step guide (Oracle Always Free VM + Terraform, and GitHub + Streamlit Community Cloud + MotherDuck for a live dashboard URL):

→ **[docs/cloud-and-free-demo.md](./docs/cloud-and-free-demo.md)**

---

## Cloud note (Phase 1)

Phase 1 ships **cloud + IaC**: Terraform in `zoomcamp/terraform/` → Oracle Always Free VM, lean `zoomcamp/` Compose, MotherDuck as warehouse. Laptop slice first. Details: [docs/cloud-and-free-demo.md](./docs/cloud-and-free-demo.md).

---

## Roadmap beyond Zoomcamp (portfolio)

Zoomcamp grades the **data pipeline + dashboard**. These phases keep the same lakehouse and show senior SWE → data/ML platform growth.

Full order: **[docs/phases.md](./docs/phases.md)**.

### Phase 1 — Zoomcamp / data platform (ship first)
- In **`zoomcamp/`**: Airflow + **dlt** + PySpark + Redpanda + PyFlink → MinIO → MotherDuck + dbt
- Streamlit: **2 tiles** (categorical + temporal) reading gold marts **directly**
- **GitHub Actions CI** on every push/PR for the graded path (see below)
- **Terraform** in `zoomcamp/terraform/` → Oracle Always Free VM, **lean** graded Compose. Get the laptop slice working first.

### Phase 1 extras — FastAPI then Next.js (not required for the course project)
- `extras/api/` FastAPI metrics against gold marts
- `extras/nextjs/` product UI that **only** calls FastAPI (no MotherDuck token in the browser)

### Phase 2 — Machine learning (~4–6 weeks after Phase 1)
- Feature marts from delay / line-status / weather gold tables  
- Baseline **XGBoost** or scikit-learn delay-risk model (leakage-aware eval)  
- **MLflow** tracking; artifacts in MinIO; batch predictions table via Airflow  
- README section: problem framing, metrics, failure modes  

### Phase 3 — Model deployment + product UI
- FastAPI **`POST /predict`** (and optional shadow vs active model)
- Next.js predict view calling that route
- Model versioning / rollback narrative via MLflow registry
- Optional small **PyTorch** sequence model only after classical ML is solid  

### FastAPI (`extras/api/` — extras backend, not graded)

Backend for **`extras/nextjs/`**. Separate from `zoomcamp/docker-compose.yml` (e.g. `http://localhost:8000` if you run extras):

| Endpoint (Phase 1) | Purpose |
|--------------------|---------|
| `GET /health` | Liveness |
| `GET /metrics/lines` | Categorical-friendly line reliability |
| `GET /metrics/reliability/daily` | Temporal series for charts |
| `GET /ops/freshness` | Source freshness SLA |

Phase 3 adds `POST /predict` using the registered model. **Zoomcamp submit:** Streamlit queries MotherDuck marts directly. Next.js is extras and must not replace those 2 tiles for graders.

### GitHub Actions CI (extra mile for Zoomcamp, expected for portfolio)

Workflow `.github/workflows/ci.yml` should at least:

1. Checkout + setup **uv** / Python  
2. Install deps (`uv sync`)  
3. **Unit tests** (`pytest`) for `zoomcamp/` producers / pure transforms  
4. **Airflow** DAG bag parse under `zoomcamp/airflow/`  
5. **dbt parse** in `zoomcamp/dbt/` (and `dbt deps` if packages used)  
6. **`docker compose -f zoomcamp/docker-compose.yml config`** validate  
7. Optional extras: FastAPI tests / OpenAPI smoke for `extras/api/`  

No secrets in logs; use GitHub Actions secrets for any tokens in deploy jobs only.

`zoomcamp/Makefile` targets (suggested): `make up`, `make down`, `make test`, `make dbt-build`. Extras may add `make api`.

---

## Author

**Zakariae Jaafari** — senior software engineer building toward data + software / platform roles.

---

## License / data credits

Code: choose a license (MIT recommended).  
Data: TfL Open Data + Open-Meteo — follow their terms; retain attribution in the dashboard footer.
