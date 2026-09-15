# TransitPulse — extras (portfolio only)

**Not part of the Zoomcamp graded project.**

Peers reviewing for the course can ignore this folder.

Phases: [docs/phases.md](../docs/phases.md). Decision: [docs/decisions/0001-extras-nextjs-fastapi.md](../docs/decisions/0001-extras-nextjs-fastapi.md).

| Path | Purpose |
|------|---------|
| `api/` | **Backend:** FastAPI metrics, later `/predict`. Reads gold marts. |
| `nextjs/` | **Product UI:** Next.js talks **only** to `api/`. No MotherDuck token in the browser. |
| `ml/` | Training, MLflow, prediction write-back |

The graded pipeline and Streamlit 2 tiles live in `../zoomcamp/`. That dashboard must **not** depend on this API for Zoomcamp submit.

Next.js is **planned**, not scaffolded yet (no app in this folder until Phase 1 extras UI).
