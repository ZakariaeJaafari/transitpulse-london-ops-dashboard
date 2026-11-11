# Pipelines

Zoomcamp grades **batch or stream**. This project **targets both**; README must say which path is primary for graders. Spec: [PROJECT.md](../PROJECT.md) §3.

**dlt** = ingest/load (Python sources → MinIO / MotherDuck). **dbt** = SQL transforms **inside** the warehouse. Do not mix the names.

## Batch (Airflow + dlt + PySpark)

Target DAG steps:

1. `ingest_tfl_static_dlt` — Journey Planner ZIP / station GTFS → MinIO bronze (and/or MD landing).
2. `ingest_open_meteo_dlt` — Open-Meteo archive/forecast → bronze/staging, incremental by date.
3. `ingest_tfl_api_snapshot_dlt` — optional Unified API snapshot (complements stream).
4. `spark_parse_static_network` — heavy parse/joins → silver Parquet on MinIO.
5. `load_motherduck` — promote silver if dlt did not already land there.
6. `dbt_build` — `dbt run` / `dbt test` on MotherDuck.

## Stream (Redpanda + PyFlink)

1. `tfl_arrivals_producer` — selected StopPoint IDs + line status → topics `tfl.arrivals`, `tfl.line_status`.
2. `weather_poll_producer` — Open-Meteo current → `weather.london`.
3. PyFlink — windows (e.g. 5-min delay **proxies** / status severity) → MinIO bronze/silver and/or staging upserts.

**As-built:** `zoomcamp/ingest/tfl_arrivals_producer.py` — pure `poll_arrivals` / `publish_arrivals(messages, send)`; `TOPIC = "tfl.arrivals"`. Host scripts: `publish_sample_arrivals.py` (sample JSON → `127.0.0.1:19092`, `flush()`), `consume_sample_arrivals.py` (consumer group `transitpulse-arrivals-demo`, `earliest`, prints two records). `poll_live_arrivals.py` GETs the current TfL Arrivals board for the four StopPoints and appends it to `tfl.arrivals` (`flush()`); it does not write `sample_data/` or the marts. `consume_new_arrivals.py` (group `transitpulse-arrivals-live`) commits its own offset; on `total 0` it prints either `watermark low … high …` or `start … high …`. `show_arrival_cursors.py` prints the partition high watermark and committed offsets for `transitpulse-arrivals-live` and `transitpulse-arrivals-append` (no poll, commit, or file write). `follow_arrivals.py` uses the same group with `auto.offset.reset=latest`, subscribes to `tfl.arrivals`, and stays in a poll loop: after a quiet second following a batch it commits, prints counts by `obs_ts`, and keeps waiting (no file write). On Ctrl+C, an open batch is committed before the consumer closes. An idle Ctrl+C does not commit. On 2026-11-11 an idle foreground Ctrl+C printed `waiting` and returned with no `committed through` line (Git Bash exit 130). `poll_live_twice.py` calls that poll twice with a 20-second gap. On 2026-11-10 it published 189 then 192, and the follower committed through 1778 then 1970. A restart printed `waiting`, then read only `obs_ts` `2026-11-10T15:55:31Z` (193 records) and committed through 2163. On 2026-11-11 `consume_new_arrivals.py` printed `total 0`. The follower was then waiting; one poll published 173 at `obs_ts` `2026-11-11T22:31:34Z` (Oxford Circus 40, King's Cross St. Pancras 71, Waterloo 40, Victoria 22), and the follower committed through 2336 without reprinting older polls. Ctrl+C after that printed no second `committed through` line. On 2026-11-05 the same group had committed through offset 599. The topic still holds older records; the commit does not delete them. `count_arrival_obs_ts.py` counts the log by `obs_ts` and does not commit. Pytest still injects `send`; no broker in CI. PyFlink: `zoomcamp/stream/print_arrivals_sample.py` — in-memory tuples + `print()` (no Kafka). `zoomcamp/stream/print_arrivals_topic.py` — bounded DataStream `KafkaSource` on `tfl.arrivals`. `zoomcamp/stream/print_arrivals_by_line.py` — batch Table SQL over the same topic (`earliest-offset` / `latest-offset`), `GROUP BY line_id` with `AVG(time_to_station_sec)` (TfL prediction mean, not delay). `write_arrivals_by_line.py` — same aggregate, `INSERT` into filesystem JSON under `stream/output/arrivals_by_line/`. It still starts at `earliest-offset` and exits 1 when a `part-*` file is already there. Two part files remain, each Victoria `n=2` at 65 seconds and Bakerloo `n=2` at 123 seconds (the frozen sample published twice; this job does not `DISTINCT`). That folder is not `marts.arrivals_by_line`. `write_arrival_updates.py` — full prediction schema from `tfl.arrivals`, `earliest-offset` through `latest-offset` (no `GROUP BY`) into `stream/output/arrival_updates/`. It exits 1 when a `part-*` file is already there. Two frozen-sample part files remain. `write_live_arrival_updates.py` is a separate bounded job (group `transitpulse-flink-live-slice`) that starts at `partition:0,offset:409` and writes `stream/output/arrival_updates_live/`. It exits 1 when a `part-*` file is already there. Before that guard, two runs on 2026-11-10 each wrote a 1754-row file from offset 409 through `obs_ts` `2026-11-10T15:55:31Z`. The original 190-row file (`obs_ts` `2026-11-05T19:13:54Z`) is still there, so the folder has three part files and is not uploaded to MinIO. `summarize_live_slice.py` exits 1 on that folder because more than one `part-*` is present. On 2026-11-05, with one file, it counted 190 predictions across 11 lines. `write_since_1970.py` (group `transitpulse-flink-since-1970`) starts at `partition:0,offset:1970` and writes `stream/output/arrival_updates_since_1970/`. On 2026-11-10 that file held 193 rows, all `obs_ts` `2026-11-10T15:55:31Z`. It is not uploaded. If a `part-*` file is already present the script exits 1 before starting Flink, because the offset is fixed and a second run would repeat those records. `summarize_since_1970.py` read that single file: 193 predictions, 11 lines (Victoria average 513.1 seconds, Bakerloo average 745.1). That folder is not the mart. `write_since_2163.py` (group `transitpulse-flink-since-2163`) starts at `partition:0,offset:2163` and writes `stream/output/arrival_updates_since_2163/`. On 2026-11-11 that file held 173 rows, all `obs_ts` `2026-11-11T22:31:34Z`. It is not uploaded. A second run exits 1 before starting Flink. `summarize_since_2163.py` read that single file: 173 predictions, 11 lines (Bakerloo 20 at 782.6 seconds, Victoria 30 at 526.7, Waterloo & City 1 at 49.0). That folder is not the mart. `append_new_arrivals.py` (group `transitpulse-arrivals-append`) starts at offset 2163 when that group has no commit, not at the earliest offset, and does not commit `transitpulse-arrivals-live`. It appends each message value as one JSON line to `stream/output/arrival_updates_appended.jsonl` (creates the file on the first write; no `part-*` file), then commits. When the start offset is already at the high watermark it prints `total 0` and does not open the file. If the group has no commit and the JSONL already has lines it exits 1. It does not poll TfL and does not upload. `follow_arrivals.py` stays stopped. That JSONL is not the mart. `summarize_appended.py` reads that JSONL (not uploaded) and prints per `obs_ts` (line rows, `subtotal`, then `total`). `zoomcamp/lake/arrival_updates_to_silver.py` copies the newest event `part-*` to MinIO `silver/arrival_updates/obs_date=2026-11-01/arrival_updates.json`. `zoomcamp/ingest/arrival_updates_dlt.py` writes `arrival_updates.parquet` beside that JSON and loads `bronze.arrival_updates`. `zoomcamp/lake/arrivals_by_line_to_silver.py` copies the newest `part-*` file to MinIO `silver/arrivals_by_line/obs_date=2026-11-01/arrivals_by_line.json` with the same boto3 client as bronze upload (Flink’s S3 plugin is not used). `zoomcamp/lake/arrivals_by_line_to_parquet.py` reads that newline-delimited JSON with DuckDB and writes `arrivals_by_line.parquet` beside it (`line_id`, `n`, `avg_predicted_seconds`); the JSON object stays. `zoomcamp/ingest/arrivals_by_line_dlt.py` loads that Parquet into MotherDuck `transitpulse.bronze.arrivals_by_line` (`write_disposition="replace"`, one row per `line_id`). `obs_date` is `2026-11-01` from the object path. `avg_predicted_seconds` stays the mean of TfL predicted seconds. Host MinIO uses the same `127.0.0.1`-before-`localhost` client as `line_status_to_silver.py`. Connector JAR under `stream/jars/`. `apache-flink` 2.2.x + Java 17+.

## Medallion on MinIO

Suggested prefixes (adjust in an ADR if the first implementation differs, then update this page):

```
s3://transitpulse/bronze/<source>/<yyyy>/<mm>/<dd>/
s3://transitpulse/silver/<entity>/
```

| Layer | Contents |
|-------|----------|
| **Bronze** | Raw API JSON, untouched ZIPs — no silent schema “fixes” |
| **Silver** | Typed Parquet, conformed `line_id`, `stop_id`, `obs_ts` |
| **Gold** | dbt marts in **MotherDuck** (canonical). Optional gold files on MinIO are not the API source. |

## As-built orchestration (Compose Airflow)

DAG `tube_status` (`zoomcamp/dags/tube_status.py`):

1. `load_tube_bronze` — `uv run python ingest/tfl_line_status_dlt.py` (reads MinIO bronze; in Compose `MINIO_ENDPOINT=http://minio:9000`; dlt writes MotherDuck `transitpulse.bronze`).
2. `build_line_status_mart` — `uv run dbt build --profiles-dir . --target motherduck --select +line_status_snapshot --no-partial-parse` in `transitPule_dbt` (staging model, mart, and contract tests). The `+` builds `staging_line_status` before `line_status_snapshot`.

DAG `weather_hourly` (`zoomcamp/dags/weather_hourly.py`):

1. `load_weather_bronze` — `uv run python ingest/weather_hourly_dlt.py` (sample JSON in the repo; dlt writes MotherDuck `transitpulse.bronze`).
2. `build_weather_mart` — `uv run dbt build --profiles-dir . --target motherduck --select +weather_hourly --no-partial-parse` (staging model, mart, and contract tests). The `+` builds `staging_weather_hourly` before `weather_hourly`.

Run `manual__2026-11-09T21:44:51+00:00` succeeded (`dbt build`, `PASS=4`). Earlier run `manual__2026-11-01T16:57:47+00:00` used `dbt run`.

DAG `arrivals_by_line` (`zoomcamp/dags/arrivals_by_line.py`) — warehouse half only (event silver JSON already on MinIO; no Flink or Redpanda in the DAG):

1. `load_arrival_updates` — `uv run python ingest/arrival_updates_dlt.py` (reads `silver/arrival_updates/obs_date=2026-11-01/arrival_updates.json`, writes sibling Parquet, dlt `replace` → `bronze.arrival_updates`; in Compose `MINIO_ENDPOINT=http://minio:9000`).
2. `build_arrivals_mart` — `uv run dbt build --profiles-dir . --target motherduck --select +arrivals_by_line +arrival_updates arrivals_by_line_matches_updates --no-partial-parse`. Builds four staging/mart tables, runs mart `not_null` / `unique` tests, then singular test `arrivals_by_line_matches_updates` (13 data tests).

Run `manual__2026-11-04T16:21:44+00:00` succeeded (load four bronze rows; `dbt build` log 13 data tests pass, singular `arrivals_by_line_matches_updates` included). Prior run `manual__2026-11-04T15:55:31+00:00` used `dbt run` only (`PASS=4`).

Both tasks export `motherduck_token` from `MOTHERDUCK_TOKEN`. Prerequisite for the Tube DAG: host `uv run python lake/upload_bronze.py` after MinIO is up (host endpoint `http://localhost:9000`). Prerequisite for `arrivals_by_line`: host or prior stream/lake steps must have placed `silver/arrival_updates/…/arrival_updates.json` on MinIO (`arrivals_by_line_dlt.py` remains for manual Flink-summary loads).

DAG `transit_batch` (`zoomcamp/dags/transit_batch.py`) — one manual chain (schedule `None`). Run `manual__2026-11-09T21:44:56+00:00` succeeded (start 21:45:00 UTC, end 21:45:52 UTC): silver Parquet, both bronze loads, then `dbt build` with `+line_status_snapshot` (`PASS=6`) and `+weather_hourly` (`PASS=4`). Earlier run `manual__2026-11-01T17:00:43+00:00` used `dbt run` for both mart tasks. The two single-entity DAGs stay for separate runs (`tube_status` uses `+line_status_snapshot`; neither writes silver):

1. `load_tube_bronze` — same as `tube_status`.
2. `write_line_status_silver` — `uv run python lake/line_status_to_silver.py` (MinIO bronze → silver Parquet; no `motherduck_token`; Compose `MINIO_ENDPOINT=http://minio:9000`).
3. `load_weather_bronze` — same as `weather_hourly`.
4. `build_line_status_mart` — same `dbt build` select as `tube_status`.
5. `build_weather_mart` — same `dbt build` select as `weather_hourly`.

## As-built scripts

| Script | Source | Destination |
|--------|--------|-------------|
| `zoomcamp/lake/upload_bronze.py` | `sample_data/tube_line.json`, `sample_data/london_weather.json` | MinIO `s3://transitpulse/bronze/.../2026/10/27/` (raw bytes) |
| `zoomcamp/lake/line_status_to_silver.py` | MinIO `bronze/tfl_line_status/2026/10/27/tube_line.json` (bytes unchanged) | MinIO `silver/line_status/status_date=2026-10-27/line_status.parquet` (DuckDB Parquet; keys `line_id`, `obs_ts`). Airflow: `transit_batch` task `write_line_status_silver`; also runnable from the host with `uv run python lake/line_status_to_silver.py`. |
| `zoomcamp/lake/arrivals_by_line_to_silver.py` | Newest `part-*` under `stream/output/arrivals_by_line/` (Flink filesystem JSON; two inserts can leave two identical files) | MinIO `silver/arrivals_by_line/obs_date=2026-11-01/arrivals_by_line.json` (JSON lines, one row per `line_id`). `avg_predicted_seconds` is the mean of TfL predicted seconds, not timetable delay. Host: `uv run python lake/arrivals_by_line_to_silver.py`. |
| `zoomcamp/lake/arrival_updates_to_silver.py` | Newest `part-*` under `stream/output/arrival_updates/` (same duplicate-`part-*` caveat) | MinIO `silver/arrival_updates/obs_date=2026-11-01/arrival_updates.json` (JSON lines, one object per prediction; `time_to_station_sec` is TfL predicted seconds). Host: `uv run python lake/arrival_updates_to_silver.py`. |
| `zoomcamp/ingest/arrival_updates_dlt.py` | MinIO `silver/arrival_updates/obs_date=2026-11-01/arrival_updates.json` (JSON kept) | MinIO `…/arrival_updates.parquet` (DuckDB `read_ndjson` + `obs_date=2026-11-01`), then MotherDuck `transitpulse.bronze.arrival_updates` (dlt `replace`; four event rows including duplicate pair). Host: `uv run python ingest/arrival_updates_dlt.py`. |
| `zoomcamp/lake/arrivals_by_line_to_parquet.py` | MinIO `silver/arrivals_by_line/obs_date=2026-11-01/arrivals_by_line.json` (left in place) | MinIO `silver/arrivals_by_line/obs_date=2026-11-01/arrivals_by_line.parquet` (DuckDB; `line_id` VARCHAR, `n` BIGINT, `avg_predicted_seconds` DOUBLE). Same MinIO httpfs settings as `line_status_to_silver.py`. Host: `uv run python lake/arrivals_by_line_to_parquet.py`. |
| `zoomcamp/ingest/arrivals_by_line_dlt.py` | MinIO `silver/arrivals_by_line/obs_date=2026-11-01/arrivals_by_line.parquet` | MotherDuck `transitpulse.bronze.arrivals_by_line` (dlt `replace`; one row per `line_id`, not one arrival). Columns `line_id`, `n`, `avg_predicted_seconds`, `obs_date` (`2026-11-01` from the path). Same bronze destination as the Tube and weather loads (`md:transitpulse`, dataset `bronze`). Host: `uv run python ingest/arrivals_by_line_dlt.py` (MinIO up; loads `zoomcamp/.env`). |
| `zoomcamp/ingest/tfl_line_status_dlt.py` | MinIO `bronze/tfl_line_status/2026/10/27/tube_line.json` | MotherDuck `transitpulse.bronze.tfl_line_status` (+ nested dlt tables) |
| `zoomcamp/ingest/weather_hourly_dlt.py` | `sample_data/london_weather_2026-10-27.json` | MotherDuck `transitpulse.bronze.weather_hourly` (one row per hourly array index) |

dbt: `models/staging/staging_line_status.sql` (Tube picker), `staging_weather_hourly.sql` (London timezone cast from bronze), `staging_arrival_updates.sql` (`DISTINCT` from `bronze.arrival_updates` to collapse duplicate sample publish), and `staging_arrivals_by_line.sql` (`GROUP BY` from `staging_arrival_updates`; mart no longer reads `bronze.arrivals_by_line`). Marts `line_status_snapshot`, `weather_hourly`, `arrivals_by_line`, and `arrival_updates` are thin `ref()` selects. On the `motherduck` target staging + mart models are tables in schema `marts`. Laptop `make dbt-build`: single `dbt build --select +line_status_snapshot +weather_hourly +arrivals_by_line +arrival_updates arrivals_by_line_matches_updates --no-partial-parse` (8 models, 19 data tests interleaved like the arrivals DAG).

## Vertical slice (Phase 1)

Ship **one** working path end-to-end before standing up every container:

ingest (dlt **or** stream) → MinIO silver (or MD staging) → dbt marts → **2 dashboard tiles** in `zoomcamp/dashboard/`.

Do **not** block the Zoomcamp slice on FastAPI. Optional later: `extras/api/` `/metrics` against the same marts.

Full stack on a free VM will not fit; lean Compose is documented in [cloud-and-free-demo.md](./cloud-and-free-demo.md).

Code lives under **`zoomcamp/`** (see [LAYOUT.md](../LAYOUT.md)). FastAPI, Next.js, and ML stay in **`extras/`**. Next.js does not belong in the Phase 1 Zoomcamp slice.

As-built: dlt into MotherDuck `transitpulse.bronze` and dbt marts on the `motherduck` target — see [status.md](./status.md).
