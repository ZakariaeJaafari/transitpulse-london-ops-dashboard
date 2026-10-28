"""Upload local sample JSON to MinIO bronze prefixes (bytes unchanged on disk)."""

from __future__ import annotations

import os
import sys
from pathlib import Path

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
from dotenv import load_dotenv

ZOOMCAMP_ROOT = Path(__file__).resolve().parent.parent
SAMPLE_DATA = ZOOMCAMP_ROOT / "sample_data"
BUCKET = "transitpulse"

BRONZE_OBJECTS: tuple[tuple[Path, str], ...] = (
    (
        SAMPLE_DATA / "tube_line.json",
        "bronze/tfl_line_status/2026/10/27/tube_line.json",
    ),
    (
        SAMPLE_DATA / "london_weather.json",
        "bronze/open_meteo/2026/10/27/london_weather.json",
    ),
)


def _require_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        print(f"Error: {name} is not set (see zoomcamp/.env.example).", file=sys.stderr)
        sys.exit(1)
    return value


def main() -> None:
    load_dotenv(ZOOMCAMP_ROOT / ".env")

    endpoint = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000").strip()
    access_key = _require_env("MINIO_ROOT_USER")
    secret_key = _require_env("MINIO_ROOT_PASSWORD")

    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )

    try:
        client.head_bucket(Bucket=BUCKET)
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code in ("404", "NoSuchBucket", "NotFound"):
            client.create_bucket(Bucket=BUCKET)
        elif code == "403":
            buckets = client.list_buckets().get("Buckets", [])
            if not any(b.get("Name") == BUCKET for b in buckets):
                client.create_bucket(Bucket=BUCKET)
        else:
            raise

    for local_path, object_key in BRONZE_OBJECTS:
        if not local_path.is_file():
            print(f"Error: missing local file {local_path}", file=sys.stderr)
            sys.exit(1)
        body = local_path.read_bytes()
        client.put_object(
            Bucket=BUCKET,
            Key=object_key,
            Body=body,
            ContentType="application/json",
        )
        print(f"Uploaded {local_path.name} → s3://{BUCKET}/{object_key} ({len(body)} bytes)")


if __name__ == "__main__":
    main()
