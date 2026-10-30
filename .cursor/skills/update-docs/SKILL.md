---
name: update-docs
description: Updates the TransitPulse living wiki after code or architecture changes. Maps edits to docs pages, refreshes docs/status.md, and updates zoomcamp/README.md for graded run steps. Use when implementing, refactoring, adding pipelines or APIs, or when the user says docs are stale.
---

# Update docs

## Protocol

In the **same change** as the implementation:

1. Open [docs/README.md](../../../docs/README.md) and apply the “If you changed… / Also update” table.
2. Always edit [docs/status.md](../../../docs/status.md): move items from Target → Shipped; drop “target” wording for what now exists.
3. Graded compose / `.env.example` / Makefile / ports → [zoomcamp/README.md](../../../zoomcamp/README.md). Root `README.md` only if the clone blurb or folder-split story changed.
4. FastAPI / Next.js / ML run steps → [extras/README.md](../../../extras/README.md).
5. Edit `PROJECT.md` only if the product spec (sources, phases, course story) changed.
6. Folder moves → [LAYOUT.md](../../../LAYOUT.md).
7. Durable design choice → new ADR under `docs/decisions/` and a row in that folder’s README.

## File map

| Change | Docs |
|--------|------|
| Ingest, Airflow, dlt, Spark, Flink, Redpanda, MinIO | `docs/pipelines.md` |
| Schemas, dbt, marts, delay proxy, StopPoints | `docs/data-model.md` |
| FastAPI, Next.js, dashboard tiles, who reads gold | `docs/serving.md`, `docs/phases.md` |
| Phase order | `docs/phases.md` |
| Terraform, Azure VM, on-VM Compose, Streamlit Cloud | `docs/azure-vm-move.md`, `docs/cloud-and-free-demo.md` |
| Course / submit | `docs/zoomcamp.md` |
| End-to-end flow | `docs/diagrams.md` |
| Folder split | `LAYOUT.md` |

## Honesty

Do not describe Compose, DAGs, or endpoints as working unless `docs/status.md` lists them as shipped. Prefer “target” vs “as-built” language. Never imply FastAPI or Next.js is required to grade `zoomcamp/`.

## Extra

If diagrams or grader-facing README screenshots would be wrong after the change, update them too.
