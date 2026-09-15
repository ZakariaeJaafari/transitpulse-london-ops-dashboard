# 0001 — Next.js product UI on FastAPI backend

- Status: accepted
- Date: 2026-09-15

## Context

The Zoomcamp dashboard is Streamlit in `zoomcamp/dashboard/`, reading MotherDuck marts directly so peers can grade without a product API. The portfolio path needs a real frontend/backend pair. `extras/nextjs/` was only a reserved folder with no app and no contract.

## Decision

- **`extras/api/` (FastAPI)** is the only backend for the extras product UI. It reads gold MotherDuck marts (metrics now, `/predict` in Phase 3).
- **`extras/nextjs/` (Next.js)** is the extras product UI. The browser talks **only** to FastAPI. No `MOTHERDUCK_TOKEN` (or TfL keys) in client code.
- **Streamlit stays the Zoomcamp dashboard.** Next.js never replaces it for graders and is not required for 24/24.
- FastAPI and Next.js stay off `zoomcamp/docker-compose.yml`.
- Build order: Phase 1 Zoomcamp slice → FastAPI metrics → Next.js on that API → ML → `/predict` + Next.js predict view. See [docs/phases.md](../phases.md).

## Consequences

- Two UIs can show the same two questions (line reliability, daily trend) from the same gold grains; only the extras UI goes through HTTP.
- Scaffolding Next.js/FastAPI is a later pass; this ADR locks the contract before code exists.
- CORS on FastAPI should allow the Next.js origin when that app is added.
