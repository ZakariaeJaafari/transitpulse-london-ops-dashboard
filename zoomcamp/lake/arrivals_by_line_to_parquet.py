"""Typed Parquet next to the arrivals_by_line silver JSON on MinIO."""

from __future__ import annotations

import sys
from pathlib import Path

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

BUCKET = "transitpulse"
JSON_KEY = "silver/arrivals_by_line/obs_date=2026-11-01/arrivals_by_line.json"
PARQUET_KEY = "silver/arrivals_by_line/obs_date=2026-11-01/arrivals_by_line.parquet"
JSON_URI = f"s3://{BUCKET}/{JSON_KEY}"
PARQUET_URI = f"s3://{BUCKET}/{PARQUET_KEY}"

COPY_SQL = f"""
COPY (
    SELECT line_id, n, avg_predicted_seconds
    FROM read_ndjson(
        '{JSON_URI}',
        columns = {{
            line_id: 'VARCHAR',
            n: 'BIGINT',
            avg_predicted_seconds: 'DOUBLE'
        }}
    )
) TO '{PARQUET_URI}' (FORMAT PARQUET)
"""

SELECT_SQL = f"""
SELECT line_id, n, avg_predicted_seconds
FROM read_parquet('{PARQUET_URI}')
"""


def main() -> None:
    load_dotenv(ZOOMCAMP_ROOT / ".env")
    access_key = _require_env("MINIO_ROOT_USER")
    secret_key = _require_env("MINIO_ROOT_PASSWORD")
    client, endpoint = _resolve_s3_client(access_key, secret_key)
    client.head_object(Bucket=BUCKET, Key=JSON_KEY)

    con = duckdb.connect()
    try:
        _configure_duckdb_s3(con, endpoint, access_key, secret_key)
        con.execute("INSTALL json; LOAD json;")
        con.execute(COPY_SQL)
        rows = con.execute(SELECT_SQL).fetchall()
    finally:
        con.close()

    client.head_object(Bucket=BUCKET, Key=JSON_KEY)
    print(f"Wrote {len(rows)} rows → {PARQUET_URI} (JSON left at {JSON_URI})")
    for line_id, n, avg_predicted_seconds in rows:
        print(f"{line_id}\t{n}\t{avg_predicted_seconds}")


if __name__ == "__main__":
    main()
