"""Publish frozen Oxford Circus arrivals to Redpanda (host script; broker optional for pytest)."""

from __future__ import annotations

import json
import sys
from pathlib import Path

from confluent_kafka import Producer

ZOOMCAMP_ROOT = Path(__file__).resolve().parent.parent
if str(ZOOMCAMP_ROOT) not in sys.path:
    sys.path.insert(0, str(ZOOMCAMP_ROOT))

from ingest.tfl_arrivals_producer import poll_arrivals, publish_arrivals
OXFORD_ARRIVALS = ZOOMCAMP_ROOT / "sample_data" / "arrivals_oxford_circus.json"
OBS_TS = "2026-11-01T17:13:00Z"
OXFORD_STOP_ID = "940GZZLUOXC"
BOOTSTRAP = "127.0.0.1:19092"


def main() -> None:
    oxford_predictions = json.loads(OXFORD_ARRIVALS.read_text(encoding="utf-8"))

    def fetch(stop_id: str) -> list[dict]:
        if stop_id == OXFORD_STOP_ID:
            return oxford_predictions
        return []

    messages = poll_arrivals(fetch, OBS_TS)
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
