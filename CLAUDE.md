@AGENTS.md

# Claude Code

- Follow [docs/README.md](docs/README.md) update protocol after implementation work.
- Project skills live in `.cursor/skills/`; read the matching `SKILL.md` before pipeline, warehouse, Zoomcamp, or docs work.
- Graded code goes in `zoomcamp/`; FastAPI (BE) + Next.js (FE) + ML in `extras/` ([LAYOUT.md](LAYOUT.md), [docs/phases.md](docs/phases.md)).
- Compose / dbt / Airflow run from `zoomcamp/` only. Never add extras to the graded Compose file.
- Next.js must call FastAPI only — no warehouse token in the client.
- Do not assume Compose, dbt, or the API exist until [docs/status.md](docs/status.md) says they shipped.
