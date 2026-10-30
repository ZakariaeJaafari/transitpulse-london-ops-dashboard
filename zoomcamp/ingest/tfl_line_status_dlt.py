"""Load TfL Tube line status snapshot JSON into DuckDB bronze with dlt."""

from __future__ import annotations

import json
import os
from pathlib import Path

import boto3
import duckdb
import dlt
from botocore.client import Config
from dotenv import load_dotenv

ZOOMCAMP_ROOT = Path(__file__).resolve().parent.parent
MOTHERDUCK_DB = "md:transitpulse"

BUCKET = "transitpulse"
BRONZE_OBJECT_KEY = "bronze/tfl_line_status/2026/10/27/tube_line.json"

PARENT_TABLE = "tfl_line_status"
LINE_STATUSES_TABLE = "tfl_line_status__line_statuses"


def _download_bronze_tube_line() -> list[dict]:
    load_dotenv(ZOOMCAMP_ROOT / ".env")

    endpoint = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000").strip()
    access_key = os.environ.get("MINIO_ROOT_USER", "").strip()
    secret_key = os.environ.get("MINIO_ROOT_PASSWORD", "").strip()
    if not access_key or not secret_key:
        raise RuntimeError(
            "MINIO_ROOT_USER and MINIO_ROOT_PASSWORD must be set (see zoomcamp/.env.example)."
        )

    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )
    response = client.get_object(Bucket=BUCKET, Key=BRONZE_OBJECT_KEY)
    body = response["Body"].read().decode("utf-8")
    payload = json.loads(body)
    if not isinstance(payload, list):
        raise TypeError(f"Expected JSON array at s3://{BUCKET}/{BRONZE_OBJECT_KEY}")
    return payload


@dlt.resource(name=PARENT_TABLE, write_disposition="replace")
def tfl_line_status():
    for record in _download_bronze_tube_line():
        yield record


def _print_post_load_report() -> None:
    con = duckdb.connect(MOTHERDUCK_DB)
    try:
        tables = con.execute(
            """
            SELECT table_schema || '.' || table_name
            FROM information_schema.tables
            WHERE table_catalog = 'transitpulse'
              AND table_schema = 'bronze'
              AND (
                table_name LIKE '_dlt%'
                OR table_name LIKE 'tfl_line_status%'
              )
            ORDER BY table_name
            """
        ).fetchall()
        print("dlt bronze tables:")
        for (name,) in tables:
            print(f"  {name}")

        parent_count = con.execute(
            f"SELECT count(*) FROM bronze.{PARENT_TABLE}"
        ).fetchone()[0]
        line_statuses_count = con.execute(
            f"SELECT count(*) FROM bronze.{LINE_STATUSES_TABLE}"
        ).fetchone()[0]

        print(f"{PARENT_TABLE} rows: {parent_count}")
        print(f"{LINE_STATUSES_TABLE} rows: {line_statuses_count}")
        print("Gold line status: run dbt for marts.line_status_snapshot")
    finally:
        con.close()


def main() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="tfl_line_status",
        destination=dlt.destinations.motherduck(credentials=MOTHERDUCK_DB),
        dataset_name="bronze",
    )
    load_info = pipeline.run(tfl_line_status())
    print(load_info)
    _print_post_load_report()


if __name__ == "__main__":
    main()
