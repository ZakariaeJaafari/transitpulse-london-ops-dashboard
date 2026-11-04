"""Upload the newest Flink arrivals_by_line part file to MinIO silver JSON."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Any

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError, EndpointConnectionError
from dotenv import load_dotenv

ZOOMCAMP_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_DIR = ZOOMCAMP_ROOT / "stream" / "output" / "arrivals_by_line"
BUCKET = "transitpulse"
OBJECT_KEY = "silver/arrivals_by_line/obs_date=2026-11-01/arrivals_by_line.json"


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


def newest_part_file(directory: Path) -> Path:
    """Return the newest part-* file written by the Flink filesystem sink."""
    parts = [path for path in directory.glob("part-*") if path.is_file()]
    if not parts:
        print(f"Error: no part-* file in {directory}", file=sys.stderr)
        sys.exit(1)
    return max(parts, key=lambda path: (path.stat().st_mtime, path.name))


def main() -> None:
    load_dotenv(ZOOMCAMP_ROOT / ".env")
    access_key = _require_env("MINIO_ROOT_USER")
    secret_key = _require_env("MINIO_ROOT_PASSWORD")

    local_path = newest_part_file(OUTPUT_DIR)
    body = local_path.read_bytes()

    client, _endpoint = _resolve_s3_client(access_key, secret_key)
    client.put_object(
        Bucket=BUCKET,
        Key=OBJECT_KEY,
        Body=body,
        ContentType="application/json",
    )
    stored = client.get_object(Bucket=BUCKET, Key=OBJECT_KEY)["Body"].read()
    print(f"Uploaded {local_path.name} → s3://{BUCKET}/{OBJECT_KEY} ({len(stored)} bytes)")
    text = stored.decode("utf-8")
    print(text, end="" if text.endswith("\n") else "\n")


if __name__ == "__main__":
    main()
