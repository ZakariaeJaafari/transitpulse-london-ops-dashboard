"""Bronze TfL line status JSON on MinIO → typed silver Parquet (DuckDB)."""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import boto3
import duckdb
from botocore.client import Config
from botocore.exceptions import ClientError, EndpointConnectionError
from dotenv import load_dotenv

ZOOMCAMP_ROOT = Path(__file__).resolve().parent.parent
if str(ZOOMCAMP_ROOT) not in sys.path:
    sys.path.insert(0, str(ZOOMCAMP_ROOT))

from ingest.line_status_proxy import line_status_row
BUCKET = "transitpulse"
BRONZE_OBJECT_KEY = "bronze/tfl_line_status/2026/10/27/tube_line.json"
SILVER_OBJECT_KEY = "silver/line_status/status_date=2026-10-27/line_status.parquet"
OBS_TS = datetime(2026, 10, 27, 0, 0, 0, tzinfo=timezone.utc)

SilverRow = tuple[str, int | None, str, bool, datetime]


def _require_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        print(f"Error: {name} is not set (see zoomcamp/.env.example).", file=sys.stderr)
        sys.exit(1)
    return value


def _loopback_endpoint(endpoint: str) -> str:
    return endpoint.replace("localhost", "127.0.0.1")


def _make_s3_client(endpoint: str, access_key: str, secret_key: str) -> Any:
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(
            signature_version="s3v4",
            connect_timeout=5,
            read_timeout=120,
        ),
        region_name="us-east-1",
    )


def _resolve_s3_client(access_key: str, secret_key: str) -> tuple[Any, str]:
    endpoint = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000").strip()
    candidates = [endpoint]
    if "localhost" in endpoint:
        loopback = _loopback_endpoint(endpoint)
        if loopback != endpoint:
            # On this Windows host localhost:9000 can hang; prefer 127.0.0.1 first.
            candidates = [loopback, endpoint]

    last_error: Exception | None = None
    for candidate in candidates:
        client = _make_s3_client(candidate, access_key, secret_key)
        try:
            client.head_bucket(Bucket=BUCKET)
            return client, candidate
        except EndpointConnectionError as exc:
            last_error = exc
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code in ("404", "NoSuchBucket", "NotFound"):
                client.create_bucket(Bucket=BUCKET)
                return client, candidate
            raise

    if last_error is not None:
        raise last_error
    raise RuntimeError("Could not connect to MinIO")


def _fetch_bronze_bytes(client: Any) -> bytes:
    response = client.get_object(Bucket=BUCKET, Key=BRONZE_OBJECT_KEY)
    return response["Body"].read()


def line_status_silver_rows(body: bytes) -> list[SilverRow]:
    """Parse bronze bytes (unchanged on disk) into silver row tuples."""
    lines = json.loads(body)
    if not isinstance(lines, list):
        raise TypeError(f"Expected JSON array at s3://{BUCKET}/{BRONZE_OBJECT_KEY}")

    rows: list[SilverRow] = []
    for line in lines:
        line_id, severity, description, disrupted = line_status_row(line)
        rows.append((line_id, severity, description, disrupted, OBS_TS))
    return rows


def _sql_string(value: str) -> str:
    return value.replace("'", "''")


def _configure_duckdb_s3(
    con: duckdb.DuckDBPyConnection,
    endpoint: str,
    access_key: str,
    secret_key: str,
) -> None:
    con.execute("INSTALL httpfs; LOAD httpfs;")
    parsed = urlparse(endpoint)
    host_port = parsed.netloc or parsed.path.removeprefix("/")
    use_ssl = parsed.scheme == "https"
    con.execute(f"SET s3_endpoint='{_sql_string(host_port)}'")
    con.execute(f"SET s3_use_ssl={'true' if use_ssl else 'false'}")
    con.execute("SET s3_url_style='path'")
    con.execute(f"SET s3_access_key_id='{_sql_string(access_key)}'")
    con.execute(f"SET s3_secret_access_key='{_sql_string(secret_key)}'")


def _write_silver_parquet(
    rows: list[SilverRow],
    endpoint: str,
    access_key: str,
    secret_key: str,
) -> None:
    con = duckdb.connect()
    try:
        _configure_duckdb_s3(con, endpoint, access_key, secret_key)
        con.execute(
            """
            CREATE TEMP TABLE line_status_silver (
                line_id VARCHAR,
                status_severity INTEGER,
                status_description VARCHAR,
                disrupted BOOLEAN,
                obs_ts TIMESTAMPTZ
            )
            """
        )
        if rows:
            con.executemany(
                "INSERT INTO line_status_silver VALUES (?, ?, ?, ?, ?)",
                rows,
            )
        target = f"s3://{BUCKET}/{SILVER_OBJECT_KEY}"
        con.execute(f"COPY line_status_silver TO '{target}' (FORMAT PARQUET)")
    finally:
        con.close()


def main() -> None:
    load_dotenv(ZOOMCAMP_ROOT / ".env")
    access_key = _require_env("MINIO_ROOT_USER")
    secret_key = _require_env("MINIO_ROOT_PASSWORD")

    client, endpoint = _resolve_s3_client(access_key, secret_key)
    body = _fetch_bronze_bytes(client)
    rows = line_status_silver_rows(body)
    _write_silver_parquet(rows, endpoint, access_key, secret_key)
    print(
        f"Wrote {len(rows)} rows → s3://{BUCKET}/{SILVER_OBJECT_KEY} "
        f"(bronze bytes unchanged at s3://{BUCKET}/{BRONZE_OBJECT_KEY})"
    )


if __name__ == "__main__":
    main()
