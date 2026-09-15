# Serving (Streamlit + FastAPI + Next.js)

Gold source: **MotherDuck dbt marts** ([data-model.md](./data-model.md)). Do not query bronze.

Phases: [phases.md](./phases.md). Decision: [0001-extras-nextjs-fastapi.md](./decisions/0001-extras-nextjs-fastapi.md).

| Surface | Path | Role |
|---------|------|------|
| **Streamlit (graded)** | `zoomcamp/dashboard/` | Zoomcamp 2 tiles. Reads marts **directly**. Must **not** call `extras/api/`. |
| **FastAPI (portfolio BE)** | `extras/api/` | Only backend for the product UI. Reads gold marts. Off `zoomcamp/docker-compose.yml`. |
| **Next.js (portfolio FE)** | `extras/nextjs/` | Product UI. **Only** talks to FastAPI. No MotherDuck token in the browser. Never the Zoomcamp dashboard. |

Prefer **Streamlit Community Cloud** for the public *graded* 2-tile demo. Next.js is the recruiter/product app after the metrics API exists.

## Dashboard (Zoomcamp: 2 tiles)

| Tile | Type | Idea | Example title |
|------|------|------|----------------|
| 1 | Categorical | Disruption / delay-proxy share by Tube line (e.g. last 7 days) | Share of disrupted intervals by Tube line (last 7 days) |
| 2 | Temporal | Daily reliability or mean delay proxy by `event_date` | Daily reliability trend — London Tube |

Optional third: rainfall vs delay scatter.

Requirements: clear titles, axis labels, footnote **Data provided by Transport for London** + Open-Meteo. Screenshots in README for peers.

## FastAPI (`extras/api/`, optional extras Compose, e.g. port 8000)

### Phase 1 extras API

| Method | Path | Returns |
|--------|------|---------|
| GET | `/health` | `{ "status": "ok" }` |
| GET | `/metrics/lines` | Per-line reliability / disruption share |
| GET | `/metrics/reliability/daily` | Time series |
| GET | `/ops/freshness` | Last success timestamps per source |
| GET | `/docs` | OpenAPI |

Rules:

- Timeouts; **503** if the warehouse is unreachable.
- No TfL keys or tokens in responses. Attribution via `/meta` or a response header.
- CORS: allow the Next.js origin only (when the UI exists). Keep this high-level until extras compose exists.

### Phase 3

| Method | Path | Returns |
|--------|------|---------|
| POST | `/predict` | Delay risk / ETA band for `{ line_id, stop_id, ts, weather_features… }` |
| GET | `/models/active` | Active MLflow model name + version |

Same feature contract as batch scoring. Do not log PII.

## Next.js (`extras/nextjs/`)

- Phase **1 extras UI**: same two questions as Streamlit (line reliability + daily trend) plus freshness, via FastAPI `/metrics/*` and `/ops/freshness`.
- Phase **3**: predict view via `POST /predict`.
- Do not query MotherDuck (or MinIO) from client-side code.
- Attribution footer same as Streamlit.

As-built: none — see [status.md](./status.md).
