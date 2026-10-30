# TransitPulse docs

Living wiki. **[PROJECT.md](../PROJECT.md)** is the Zoomcamp product spec (stable). This folder is the as-built companion: update it whenever code, contracts, run steps, or course deliverables change.

Agent briefing: [AGENTS.md](../AGENTS.md). Cursor workflows: `.cursor/skills/` (especially `update-docs`).

## Index

| Doc | Purpose |
|-----|---------|
| [status.md](./status.md) | As-built vs target — start here |
| [phases.md](./phases.md) | Build order: Zoomcamp → FastAPI → Next.js → ML → `/predict` |
| [diagrams.md](./diagrams.md) | A→Z, runtime, phases, free deploy |
| [data-model.md](./data-model.md) | Keys, grains, partition/cluster, canonical gold |
| [pipelines.md](./pipelines.md) | Batch vs stream, MinIO prefixes, dlt vs dbt |
| [serving.md](./serving.md) | Streamlit (graded) + FastAPI BE + Next.js FE |
| [zoomcamp.md](./zoomcamp.md) | Course deliverables |
| [cloud-and-free-demo.md](./cloud-and-free-demo.md) | Terraform VM + Streamlit public demo |
| [azure-vm-move.md](./azure-vm-move.md) | Certification log: laptop slice → Azure VM |
| [decisions/](./decisions/README.md) | ADRs |

Human landing page: [README.md](../README.md). Folder split: [LAYOUT.md](../LAYOUT.md) (`zoomcamp/` graded, `extras/` portfolio).

---

## When to update (protocol)

In the **same change** as the code or architecture edit:

1. Touch the matching wiki page(s) in the table below.
2. Always refresh [status.md](./status.md) (what exists vs still target).
3. Edit **[zoomcamp/README.md](../zoomcamp/README.md)** if compose, `.env.example`, Makefile, or graded ports change. Edit root [README.md](../README.md) if the clone/run blurb or folder-split story changes.
4. Edit [extras/README.md](../extras/README.md) if FastAPI/Next.js/ML run steps change.
5. Edit [PROJECT.md](../PROJECT.md) **only** if the product spec itself changed (sources, phases, course story).
6. Edit [LAYOUT.md](../LAYOUT.md) if folders move.

| If you changed… | Also update |
|-----------------|-------------|
| Ingest, Airflow, dlt, Spark, Flink, Redpanda, MinIO paths | [pipelines.md](./pipelines.md), [status.md](./status.md) |
| Schemas, dbt models, marts, delay proxy, StopPoint set | [data-model.md](./data-model.md), [status.md](./status.md) |
| FastAPI routes, dashboard tiles, Next.js, who reads gold | [serving.md](./serving.md), [phases.md](./phases.md), [status.md](./status.md); extras README if API/UI run steps change |
| Phase order / what ships when | [phases.md](./phases.md), [status.md](./status.md) |
| Compose, Makefile, `.env.example`, ports | [zoomcamp/README.md](../zoomcamp/README.md), root [README.md](../README.md) how-to-run, [status.md](./status.md) |
| Folder split / where code lives | [LAYOUT.md](../LAYOUT.md) |
| Terraform, Azure VM, on-VM Compose, Streamlit Cloud | [azure-vm-move.md](./azure-vm-move.md), [cloud-and-free-demo.md](./cloud-and-free-demo.md), [status.md](./status.md) |
| Course deliverables, submit checklist | [zoomcamp.md](./zoomcamp.md) |
| A durable design choice | New ADR under [decisions/](./decisions/README.md) |
| End-to-end flow | [diagrams.md](./diagrams.md) |

Do not leave “target” language in status after something actually ships.
