# TransitPulse — Diagrams (idea → deploy → predict)

Simple action / flow diagrams for peers and your future self. Rendered automatically on GitHub (Mermaid).

**Ingest note:** batch/API loads use **dlt**; warehouse transforms use **dbt**; stream path stays Redpanda + PyFlink.

---

## 1. End-to-end action flow (A → Z)

```mermaid
flowchart TD
  A[Idea: London ops reliability + weather] --> B[Pick sources: TfL + Open-Meteo]
  B --> C[Design: lake MinIO + DWH MotherDuck]
  C --> D[Implement batch: Airflow + dlt + PySpark]
  C --> E[Implement stream: producers + Redpanda + PyFlink]
  D --> F[dlt lands bronze; Spark builds silver on MinIO]
  E --> F
  F --> G[Load warehouse MotherDuck]
  G --> H[Transform: dbt staging → marts]
  H --> I["Dashboard 2 tiles zoomcamp/dashboard"]
  I --> K[CI: GitHub Actions on zoomcamp]
  K --> L[Deploy free: Streamlit Cloud + optional Oracle VM]
  H --> J["extras FastAPI /health + /metrics"]
  J --> NJS["extras Next.js product UI"]
  H --> M["extras Phase 2: features + train XGBoost"]
  M --> N[MLflow registry + artifacts on MinIO]
  N --> O["extras Phase 3: POST /predict + Next.js predict"]
  L --> P[Peers / recruiters open live demo]
  NJS --> P
  O --> P
```

Zoomcamp submit is **A → I → K** (optionally L). Nodes tagged extras are portfolio-only.

---

## 2. Data path (runtime)

```mermaid
flowchart LR
  subgraph Sources
    T1[TfL timetables / GTFS ZIP]
    T2[TfL Unified API arrivals + line status]
    W[Open-Meteo forecast + archive]
  end

  subgraph Batch
    AF[Airflow DAGs]
    DLT[dlt pipelines]
    SP[PySpark jobs]
  end

  subgraph Stream
    PR[Producers]
    RP[Redpanda]
    FL[PyFlink windows]
  end

  subgraph Lake["Data lake = MinIO"]
    BR[bronze]
    SI[silver]
    GO[gold files optional]
  end

  subgraph DWH["Warehouse = MotherDuck"]
    DBT[dbt models + tests]
    MARTS[gold marts]
  end

  subgraph gradedServe ["zoomcamp dashboard"]
    UI[Streamlit 2 tiles]
  end

  subgraph extrasServe ["extras portfolio"]
    API[FastAPI]
    NJS[Next.js]
    ML[MLflow]
  end

  T1 --> AF --> DLT --> BR
  DLT --> MARTS
  AF --> SP --> SI
  BR --> SI
  T2 --> PR --> RP --> FL --> BR
  W --> AF
  W --> DLT
  W --> PR
  SI --> DBT --> MARTS
  MARTS --> UI
  MARTS --> API
  API --> NJS
  MARTS --> ML
```

---

## 3. Lifecycle phases (when you do what)

```mermaid
flowchart LR
  P0[Phase 0 spec] --> P1[Phase 1 Zoomcamp: pipeline + 2 tiles + CI + Terraform VM]
  P1 --> P1e[Phase 1 extras: FastAPI metrics]
  P1e --> P1ui[Phase 1 extras: Next.js on API]
  P1 --> P2[Phase 2 extras: ML train + MLflow]
  P1e --> P2
  P2 --> P3[Phase 3: /predict + Next.js predict]
  P1ui --> P3
```

Full table: [phases.md](./phases.md).

| Phase | You ship | Not required for Zoomcamp submit |
|-------|----------|----------------------------------|
| **1 Zoomcamp** | Ingest → lake → DWH → dbt → 2 Streamlit tiles + CI + **Terraform free VM** in `zoomcamp/` | FastAPI, Next.js, ML |
| **1 extras API** | FastAPI `/metrics/*` | Graded dashboard |
| **1 extras UI** | Next.js → FastAPI only | Replacing Streamlit for graders |
| **2** | Features, XGBoost, MLflow, batch scores in `extras/ml/` | Public `/predict` |
| **3** | `POST /predict` + Next.js predict view | PyTorch |

---

## 4. Free deploy options (from `cloud-and-free-demo.md`)

```mermaid
flowchart TD
  REPO[GitHub repo + Actions CI] --> MD[MotherDuck free gold marts]
  MD --> SC[Streamlit Community Cloud = public dashboard URL]
  MD --> FA[Optional extras FastAPI]
  FA --> NX[Optional extras Next.js]
  REPO --> TF[Terraform]
  TF --> OCI[Oracle Always Free VM]
  OCI --> COMPOSE["zoomcamp Compose: Airflow dlt MinIO Redpanda Flink dashboard"]
  COMPOSE --> MD
  SC --> PEERS[Peer reviewers click link]
  NX --> PEERS
```

---

## How to read this

1. **Diagram 1** = project journey from idea to prediction.
2. **Diagram 2** = where bytes move at runtime.
3. **Diagram 3** = build order.
4. **Diagram 4** = $0 internet presence + Terraform VM path.

Details: [README.md](../README.md) · [PROJECT.md](../PROJECT.md) · [phases.md](./phases.md) · [cloud-and-free-demo.md](./cloud-and-free-demo.md) · [docs index](./README.md)
