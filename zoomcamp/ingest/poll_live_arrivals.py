"""Poll live TfL StopPoint arrivals and publish to Redpanda (host script)."""

from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from confluent_kafka import Producer
from dotenv import load_dotenv

ZOOMCAMP_ROOT = Path(__file__).resolve().parent.parent
if str(ZOOMCAMP_ROOT) not in sys.path:
    sys.path.insert(0, str(ZOOMCAMP_ROOT))

from ingest.stoppoints import STOPPOINTS
from ingest.tfl_arrivals_producer import poll_arrivals, publish_arrivals

TFL_ARRIVALS_URL = "https://api.tfl.gov.uk/StopPoint/{stop_id}/Arrivals"
BOOTSTRAP = "127.0.0.1:19092"


def _require_tfl_app_key() -> str:
    key = os.environ.get("TFL_APP_KEY", "").strip()
    if not key:
        print(
            "Error: TFL_APP_KEY is not set. Add it to zoomcamp/.env or export it in your shell.",
            file=sys.stderr,
        )
        sys.exit(1)
    return key


def _get(url: str) -> tuple[int, bytes]:
    request = urllib.request.Request(url, headers={"User-Agent": "TransitPulse/0.1"})
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            return response.status, response.read()
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read()


def _require_http_ok(label: str, status: int) -> None:
    if status == 200:
        return
    print(f"Error: {label}: HTTP {status}", file=sys.stderr)
    sys.exit(1)


def main() -> None:
    load_dotenv(ZOOMCAMP_ROOT / ".env")
    app_key = _require_tfl_app_key()
    obs_ts = datetime.now(timezone.utc).replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")

    def fetch(stop_id: str) -> list[dict[str, Any]]:
        url = (
            TFL_ARRIVALS_URL.format(stop_id=stop_id)
            + "?"
            + urllib.parse.urlencode({"app_key": app_key})
        )
        status, body = _get(url)
        _require_http_ok(f"TfL StopPoint {stop_id} Arrivals", status)
        payload = json.loads(body)
        if not isinstance(payload, list):
            print(
                f"Error: TfL StopPoint {stop_id} Arrivals: expected JSON list",
                file=sys.stderr,
            )
            sys.exit(1)
        return payload

    messages = poll_arrivals(fetch, obs_ts)
    counts = {stop["stop_id"]: 0 for stop in STOPPOINTS}
    for message in messages:
        counts[message["stop_id"]] += 1

    for stop in STOPPOINTS:
        stop_id = stop["stop_id"]
        print(f"{stop_id} {stop['name']} {counts[stop_id]}")

    print(f"total {len(messages)}")

    producer = Producer({"bootstrap.servers": BOOTSTRAP})

    def send(topic: str, key: str, value: dict) -> None:
        producer.produce(
            topic,
            key=key.encode("utf-8"),
            value=json.dumps(value).encode("utf-8"),
        )

    count = publish_arrivals(messages, send)
    producer.flush()
    print(f"published {count}")


if __name__ == "__main__":
    main()
