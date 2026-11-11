# TransitPulse — Zoomcamp scope

This folder is the **DataTalksClub DE Zoomcamp course project**.

## Graded path

Dataset (TfL + Open-Meteo) → **data lake (MinIO)** → **warehouse (MotherDuck)** → **dbt** → **dashboard (2 tiles)**.

**Batch (this slice):** Airflow DAGs `tube_status`, `weather_hourly`, `arrivals_by_line`, and combined `transit_batch` run dlt into MotherDuck `transitpulse.bronze`. DAG `arrivals_by_line` reloads MinIO silver `arrival_updates` JSON (dlt) and runs dbt `+arrivals_by_line +arrival_updates` (no Flink/Redpanda in the DAG). dbt `--target motherduck` builds staging then mart for each tile: `marts.staging_line_status` → `marts.line_status_snapshot` (`--select +line_status_snapshot`) and `marts.staging_weather_hourly` → `marts.weather_hourly` (`--select +weather_hourly`). The Tube load reads MinIO. The weather load reads `sample_data/`. Use `transit_batch` for one trigger: tube bronze → line-status silver Parquet on MinIO → weather bronze → both staging models and both marts.

**Stream (laptop, partial):** the warehouse path is still the frozen Oxford Circus sample. Sample arrivals land on Redpanda. `stream/write_arrival_updates.py` reads the topic from the earliest offset and writes local JSON; it exits 1 when a part file is already there (two frozen-sample files remain). Silver upload and `ingest/arrival_updates_dlt.py` load that frozen folder into `bronze.arrival_updates`. The line mart is a dbt `GROUP BY` of those events (Victoria `n=1` at 65 seconds, Bakerloo `n=1` at 123 seconds, `obs_date` 2026-11-01). `dbt show --no-partial-parse` on 2026-11-10 still returned those rows. `stream/write_arrivals_by_line.py` still starts at the earliest offset, exits 1 when a part file is already there, and its two files are the sample counted twice (Victoria `n=2` at 65, Bakerloo `n=2` at 123). That folder is not the mart. `ingest/poll_live_arrivals.py` can append the current TfL board to the same topic. `stream/write_live_arrival_updates.py` still starts at offset 409 and exits 1 when a part file is already there. Before that guard, two runs each wrote a 1754-row file from offset 409 through `obs_ts` `2026-11-10T15:55:31Z`. The original 190-row file is still there, so `stream/output/arrival_updates_live/` has three part files; it is not uploaded and is not the mart. `stream/summarize_live_slice.py` exits 1 on that folder. `ingest/follow_arrivals.py` waits on group `transitpulse-arrivals-live` and commits after each quiet batch. On Ctrl+C it commits an open batch and does not commit when no batch is open. On 2026-11-11 it was waiting, one poll published 173 predictions at `obs_ts` `2026-11-11T22:31:34Z`, and the follower committed through 2336. `stream/write_since_1970.py` reads offset 1970 into `stream/output/arrival_updates_since_1970/` (193 rows at `obs_ts` `2026-11-10T15:55:31Z`, not uploaded) and exits 1 if a part file is already there. `stream/summarize_since_1970.py` read that file: 193 predictions, 11 lines (Victoria average 513.1 seconds, Bakerloo average 745.1). That folder is not the mart. `stream/write_since_2163.py` reads offset 2163 into `stream/output/arrival_updates_since_2163/` (173 rows at `obs_ts` `2026-11-11T22:31:34Z`, not uploaded) and exits 1 if a part file is already there. `stream/summarize_since_2163.py` read that file: 173 predictions, 11 lines (Victoria average 526.7 seconds, Bakerloo average 782.6). That folder is not the mart. `ingest/append_new_arrivals.py` uses group `transitpulse-arrivals-append` (it does not commit `transitpulse-arrivals-live`). With no commit it starts at offset 2163, appends JSON lines to `stream/output/arrival_updates_appended.jsonl`, and commits after the write. A second copy of that slice exits 1 if the group has no commit and the file already has lines. At the high watermark it prints `total 0` and does not open the file. It does not poll TfL and does not upload. `follow_arrivals.py` stays stopped. That JSONL is not the mart. Flink is not a Compose service and is not a continuous windowed job. Flink does not write S3 itself. PySpark is not in this slice. The older line-summary files (`arrivals_by_line_to_silver.py`, `arrivals_by_line_to_parquet.py`, `arrivals_by_line_dlt.py`) remain for a manual Flink-aggregate load. `avg_predicted_seconds` is still TfL predicted seconds.

![TransitPulse Tube status and London weather tiles](dashboard/tiles.png)

## How to run

Commands below assume you are **in this folder** (`zoomcamp/`).

**Makefile** (same folder):

```bash
make up          # docker compose up -d --build --wait
make test        # uv sync --all-groups && uv run pytest
make dbt-build   # .env next to the Makefile; dbt build (+line_status_snapshot +weather_hourly +arrivals_by_line +arrival_updates + contract test)
```

**As-built (local laptop):**

MinIO lake (API `9000`, console `9001`) + Airflow (`8080`) + Redpanda (Kafka on host `127.0.0.1:19092`):

```bash
cd zoomcamp
cp .env.example .env   # add MINIO_ROOT_USER / MINIO_ROOT_PASSWORD (and other keys)
docker compose up -d --build --wait   # waits until Airflow /health is OK (~1–3 min first start)
docker compose up -d redpanda
docker compose exec redpanda rpk cluster health
docker compose exec redpanda rpk topic create tfl.arrivals -p 1 -r 1
docker compose exec redpanda rpk topic list
# Host Kafka bootstrap: 127.0.0.1:19092 (not localhost on this Windows host — IPv6/wslrelay)
uv run python ingest/publish_sample_arrivals.py
uv run python ingest/consume_sample_arrivals.py
uv run python ingest/poll_live_arrivals.py   # current TfL board → tfl.arrivals; does not reload marts
uv run python ingest/consume_new_arrivals.py # group transitpulse-arrivals-live; commits its cursor
uv run python ingest/show_arrival_cursors.py  # high watermark + live/append commits; read-only
uv run python ingest/follow_arrivals.py     # same group; latest; commits a quiet batch, and an open batch on Ctrl+C
uv run python ingest/append_new_arrivals.py # group transitpulse-arrivals-append; offset 2163 if no commit; JSONL append; does not commit live
uv run python ingest/poll_live_twice.py      # two polls, 20s apart; does not reload marts
uv run python ingest/count_arrival_obs_ts.py # count by obs_ts; does not commit
docker compose exec redpanda rpk topic consume tfl.arrivals -o start -n 2 -f 'key=%k%n%v%n'
# PyFlink (Java 17+; download Kafka connector JAR — see stream/jars/README.md):
uv run python stream/print_arrivals_sample.py
uv run python stream/print_arrivals_topic.py   # Redpanda up; reads tfl.arrivals once (bounded)
uv run python stream/print_arrivals_by_line.py # batch SQL: one row per line_id (avg predicted sec)
uv run python stream/write_arrivals_by_line.py # earliest-offset aggregate → stream/output/arrivals_by_line/ (exits 1 if a part file exists)
uv run python stream/write_arrival_updates.py  # earliest-offset; one JSON row per prediction → stream/output/arrival_updates/ (exits 1 if a part file exists)
uv run python stream/write_live_arrival_updates.py  # offset 409 → stream/output/arrival_updates_live/ (not MinIO; exits 1 if a part file exists)
uv run python stream/write_since_1970.py  # offset 1970 → stream/output/arrival_updates_since_1970/ (not MinIO; exits 1 if a part file exists)
uv run python stream/summarize_live_slice.py # line counts for the offset-409 folder; exits 1 unless exactly one part file
uv run python stream/write_since_2163.py  # offset 2163 → stream/output/arrival_updates_since_2163/ (not MinIO; exits 1 if a part file exists)
uv run python stream/summarize_since_1970.py # line counts for the offset-1970 folder; not the mart
uv run python stream/summarize_since_2163.py # line counts for the offset-2163 folder; not the mart
uv run python stream/summarize_appended.py   # line counts for arrival_updates_appended.jsonl; not the mart
uv sync
uv run python lake/upload_bronze.py   # host → http://localhost:9000
uv run python lake/line_status_to_silver.py   # bronze JSON → silver Parquet (DuckDB); uses 127.0.0.1 if localhost fails
uv run python lake/arrivals_by_line_to_silver.py  # newest part-* → silver/arrivals_by_line/…json
uv run python lake/arrival_updates_to_silver.py   # newest part-* → silver/arrival_updates/…json
uv run python lake/arrivals_by_line_to_parquet.py  # JSON lines → typed Parquet; JSON object stays
uv run python ingest/arrivals_by_line_dlt.py  # that Parquet → MotherDuck bronze.arrivals_by_line (loads .env)
uv run python ingest/arrival_updates_dlt.py   # event JSON → Parquet → bronze.arrival_updates (loads .env)
# Console: http://localhost:9001 → bucket transitpulse → bronze/…, silver/line_status/…, and silver/arrivals_by_line/…
# Airflow UI: http://127.0.0.1:8080 — login admin / admin (override AIRFLOW_WWW_USER / AIRFLOW_WWW_USER_PASSWORD in .env)
# On this Windows host, http://localhost:8080 is reset (IPv6 localhost hits wslrelay). Use 127.0.0.1.
# Unpause DAG transit_batch (or tube_status / weather_hourly / arrivals_by_line separately)
```

Airflow runs in Docker (not on Windows natively). The repo is mounted at `/opt/transitpulse/zoomcamp`; DAG tasks use `http://minio:9000` for the lake. Keep `MINIO_ENDPOINT=http://localhost:9000` in `.env` for host commands.

MotherDuck warehouse path (export token before dlt, dbt, and dashboard):

```bash
cd zoomcamp
uv sync
set -a && source .env && set +a && export motherduck_token="$MOTHERDUCK_TOKEN"
uv run python ingest/weather_hourly_dlt.py
uv run python ingest/tfl_line_status_dlt.py   # MinIO up for lake read
cd transitPule_dbt
uv run dbt run --profiles-dir . --target motherduck --select +line_status_snapshot +weather_hourly --no-partial-parse
uv run dbt test --profiles-dir . --target motherduck --select line_status_snapshot weather_hourly
cd ..
uv run streamlit run dashboard/app.py
```

Local file `data/transitpulse.duckdb` is for dbt **dev** only. **dlt** loads bronze into MotherDuck `transitpulse.bronze.*`; dbt **motherduck** builds staging tables and marts in schema `marts`. Streamlit reads **MotherDuck marts only** — graded tiles: `line_status_snapshot`, `weather_hourly`; a third section charts `arrivals_by_line` and lists `arrival_updates` for the 2026-11-01 sample (mean predicted seconds by line, not the live file under `stream/output/arrival_updates_live/`). Chart and table calls use `width="stretch"`. On this Windows host open **http://127.0.0.1:8501**.

Tests (no `.env` required):

```bash
cd zoomcamp
uv sync --all-groups
uv run pytest
```

**CI also runs** (no MotherDuck token, no `.env`, no Airflow webserver): `dbt parse --target dev`; `docker compose config --quiet` with `MINIO_ROOT_USER=ci` / `MINIO_ROOT_PASSWORD=ci`; `DagBag('dags')` in `apache/airflow:2.10.4-python3.12` with the repo mounted at `/opt/transitpulse/zoomcamp`.

**Target (not all shipped yet):**

```bash
cd zoomcamp
cp .env.example .env   # TFL_APP_KEY, MOTHERDUCK_TOKEN, MinIO keys
docker compose up -d
# Airflow → dlt → dbt → open dashboard/ (2 tiles)
```

`.env.example`, Compose, dbt, and Terraform (`terraform/` — Phase 1 Azure credit VM) all live **here**, not at the repo root.

**Terraform (Azure VM, $100 credit):**

```bash
az login
cd zoomcamp/terraform
cp terraform.tfvars.example terraform.tfvars   # subscription id, your IP, SSH public key; file is gitignored
terraform init
terraform validate
# spends the credit: terraform plan && terraform apply
```

Details: [terraform/README.md](terraform/README.md) · [docs/cloud-and-free-demo.md](../docs/cloud-and-free-demo.md).

The VM is applied (`austriaeast`, zone 1, `Standard_B2s_v2`). MinIO, Airflow, and a successful `tube_status` run are recorded in [docs/azure-vm-move.md](../docs/azure-vm-move.md), including the laptop SSH tunnels (Airflow `:8081`, MinIO `:19001`). Deallocate the VM when idle.

Living docs: [docs/zoomcamp.md](../docs/zoomcamp.md) · [docs/cloud-and-free-demo.md](../docs/cloud-and-free-demo.md) · [docs/status.md](../docs/status.md)

## Out of scope here

See `../extras/` for FastAPI (backend), Next.js (product UI), ML/`/predict` — **not required for peer grading**.
