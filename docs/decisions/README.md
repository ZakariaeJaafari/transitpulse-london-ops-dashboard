# Architecture decision records

Add an ADR when a choice will be hard to reverse or will confuse later agents (ingest tool, delay definition, dashboard product, cloud VM shape).

Template (new file `NNNN-short-title.md`):

```markdown
# NNNN — Title

- Status: proposed | accepted | superseded
- Date: YYYY-MM-DD

## Context
## Decision
## Consequences
```

## Index

| ID | Title | Status |
|----|-------|--------|
| [0001](./0001-extras-nextjs-fastapi.md) | Next.js product UI on FastAPI backend | accepted |

Expected later ADRs:

- Batch + stream both in-repo vs one graded path
- dlt vs hand-rolled ingest
- Delay **proxy** vs timetable join
- Streamlit vs Metabase for the *graded* public demo
- MinIO lake + MotherDuck warehouse split
