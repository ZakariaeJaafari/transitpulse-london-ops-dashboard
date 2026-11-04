"""Load arrivals-by-line silver Parquet into MotherDuck bronze with dlt."""

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
PARQUET_KEY = "silver/arrivals_by_line/obs_date=2026-11-01/arrivals_by_line.parquet"
PARQUET_URI = f"s3://{BUCKET}/{PARQUET_KEY}"
OBS_DATE = "2026-11-01"
RESOURCE_NAME = "arrivals_by_line"


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


def _read_parquet_rows(endpoint: str, access_key: str, secret_key: str) -> list[dict]:
    con = duckdb.connect()
    try:
        _configure_duckdb_s3(con, endpoint, access_key, secret_key)
        rows = con.execute(
            f"""
            SELECT line_id, n, avg_predicted_seconds
            FROM read_parquet('{PARQUET_URI}')
            """
        ).fetchall()
    finally:
        con.close()

    return [
        {
            "line_id": str(line_id),
            "n": int(n),
            "avg_predicted_seconds": float(avg_predicted_seconds),
            "obs_date": OBS_DATE,
        }
        for line_id, n, avg_predicted_seconds in rows
    ]


@dlt.resource(name=RESOURCE_NAME, write_disposition="replace")
def arrivals_by_line():
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
            SELECT line_id, n, avg_predicted_seconds
            FROM bronze.{RESOURCE_NAME}
            ORDER BY line_id
            """
        ).fetchall()
        print(f"{RESOURCE_NAME} rows: {row_count}")
        for line_id, n, avg_predicted_seconds in rows:
            print(f"{line_id}\t{n}\t{avg_predicted_seconds}")
    finally:
        con.close()


def main() -> None:
    _load_env()
    pipeline = dlt.pipeline(
        pipeline_name=RESOURCE_NAME,
        destination=dlt.destinations.motherduck(credentials=MOTHERDUCK_DB),
        dataset_name="bronze",
    )
    load_info = pipeline.run(arrivals_by_line())
    print(load_info)
    _print_post_load_report()


if __name__ == "__main__":
    main()
