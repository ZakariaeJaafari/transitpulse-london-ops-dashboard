"""Silver arrival_updates JSON → Parquet on MinIO, then MotherDuck bronze via dlt."""

from __future__ import annotations

import os
import sys
from pathlib import Path

import dlt
import duckdb
from dotenv import load_dotenv

ZOOMCAMP_ROOT = Path(__file__).resolve().parent.parent
if str(ZOOMCAMP_ROOT) not in sys.path:
    sys.path.insert(0, str(ZOOMCAMP_ROOT))

from lake.line_status_to_silver import (  # noqa: E402
    _configure_duckdb_s3,
    _require_env,
    _resolve_s3_client,
)

MOTHERDUCK_DB = "md:transitpulse"
BUCKET = "transitpulse"
JSON_KEY = "silver/arrival_updates/obs_date=2026-11-01/arrival_updates.json"
PARQUET_KEY = "silver/arrival_updates/obs_date=2026-11-01/arrival_updates.parquet"
JSON_URI = f"s3://{BUCKET}/{JSON_KEY}"
PARQUET_URI = f"s3://{BUCKET}/{PARQUET_KEY}"
OBS_DATE = "2026-11-01"
RESOURCE_NAME = "arrival_updates"

COPY_SQL = f"""
COPY (
    SELECT
        stop_id,
        line_id,
        vehicle_id,
        expected_arrival,
        time_to_station_sec,
        towards,
        obs_ts,
        DATE '{OBS_DATE}' AS obs_date
    FROM read_ndjson(
        '{JSON_URI}',
        columns = {{
            stop_id: 'VARCHAR',
            line_id: 'VARCHAR',
            vehicle_id: 'VARCHAR',
            expected_arrival: 'VARCHAR',
            time_to_station_sec: 'INTEGER',
            towards: 'VARCHAR',
            obs_ts: 'VARCHAR'
        }}
    )
) TO '{PARQUET_URI}' (FORMAT PARQUET)
"""

SELECT_PARQUET_SQL = f"""
SELECT
    stop_id,
    line_id,
    vehicle_id,
    expected_arrival,
    time_to_station_sec,
    towards,
    obs_ts,
    obs_date
FROM read_parquet('{PARQUET_URI}')
"""


def _ensure_motherduck_token() -> None:
    """DuckDB and dlt read motherduck_token. Copy from MOTHERDUCK_TOKEN if empty."""
    token = os.environ.get("motherduck_token", "").strip()
    if token:
        return
    fallback = os.environ.get("MOTHERDUCK_TOKEN", "").strip()
    if not fallback:
        raise RuntimeError(
            "motherduck_token is empty and MOTHERDUCK_TOKEN is not set "
            "(see zoomcamp/.env.example)."
        )
    os.environ["motherduck_token"] = fallback


def _load_env() -> None:
    load_dotenv(ZOOMCAMP_ROOT / ".env")
    _ensure_motherduck_token()


def _write_parquet(endpoint: str, access_key: str, secret_key: str) -> int:
    con = duckdb.connect()
    try:
        _configure_duckdb_s3(con, endpoint, access_key, secret_key)
        con.execute("INSTALL json; LOAD json;")
        con.execute(COPY_SQL)
        row_count = con.execute(
            f"SELECT count(*) FROM read_parquet('{PARQUET_URI}')"
        ).fetchone()[0]
    finally:
        con.close()
    return int(row_count)


def _read_parquet_rows(endpoint: str, access_key: str, secret_key: str) -> list[dict]:
    con = duckdb.connect()
    try:
        _configure_duckdb_s3(con, endpoint, access_key, secret_key)
        rows = con.execute(SELECT_PARQUET_SQL).fetchall()
    finally:
        con.close()

    return [
        {
            "stop_id": str(stop_id),
            "line_id": str(line_id),
            "vehicle_id": str(vehicle_id),
            "expected_arrival": str(expected_arrival),
            "time_to_station_sec": int(time_to_station_sec),
            "towards": str(towards),
            "obs_ts": str(obs_ts),
            "obs_date": str(obs_date)[:10] if obs_date is not None else OBS_DATE,
        }
        for (
            stop_id,
            line_id,
            vehicle_id,
            expected_arrival,
            time_to_station_sec,
            towards,
            obs_ts,
            obs_date,
        ) in rows
    ]


@dlt.resource(name=RESOURCE_NAME, write_disposition="replace")
def arrival_updates():
    _load_env()
    access_key = _require_env("MINIO_ROOT_USER")
    secret_key = _require_env("MINIO_ROOT_PASSWORD")
    _client, endpoint = _resolve_s3_client(access_key, secret_key)
    for record in _read_parquet_rows(endpoint, access_key, secret_key):
        yield record


def _print_post_load_report() -> None:
    con = duckdb.connect(MOTHERDUCK_DB)
    try:
        row_count = con.execute(
            f"SELECT count(*) FROM bronze.{RESOURCE_NAME}"
        ).fetchone()[0]
        rows = con.execute(
            f"""
            SELECT line_id, vehicle_id, time_to_station_sec
            FROM bronze.{RESOURCE_NAME}
            ORDER BY line_id, vehicle_id
            """
        ).fetchall()
        print(f"{RESOURCE_NAME} rows: {row_count}")
        for line_id, vehicle_id, time_to_station_sec in rows:
            print(f"{line_id}\t{vehicle_id}\t{time_to_station_sec}")
    finally:
        con.close()


def main() -> None:
    _load_env()
    access_key = _require_env("MINIO_ROOT_USER")
    secret_key = _require_env("MINIO_ROOT_PASSWORD")
    client, endpoint = _resolve_s3_client(access_key, secret_key)
    client.head_object(Bucket=BUCKET, Key=JSON_KEY)

    parquet_rows = _write_parquet(endpoint, access_key, secret_key)
    client.head_object(Bucket=BUCKET, Key=JSON_KEY)
    print(
        f"Wrote {parquet_rows} rows → {PARQUET_URI} (JSON left at {JSON_URI})"
    )

    pipeline = dlt.pipeline(
        pipeline_name=RESOURCE_NAME,
        destination=dlt.destinations.motherduck(credentials=MOTHERDUCK_DB),
        dataset_name="bronze",
    )
    load_info = pipeline.run(arrival_updates())
    print(load_info)
    _print_post_load_report()


if __name__ == "__main__":
    main()
