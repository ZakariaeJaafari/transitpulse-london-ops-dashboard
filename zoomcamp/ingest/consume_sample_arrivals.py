"""Read two sample arrivals from Redpanda (host script; broker optional for pytest)."""

from __future__ import annotations

import json
import sys
from pathlib import Path

from confluent_kafka import Consumer, KafkaError, KafkaException

ZOOMCAMP_ROOT = Path(__file__).resolve().parent.parent
if str(ZOOMCAMP_ROOT) not in sys.path:
    sys.path.insert(0, str(ZOOMCAMP_ROOT))

from ingest.tfl_arrivals_producer import TOPIC

BOOTSTRAP = "127.0.0.1:19092"
GROUP_ID = "transitpulse-arrivals-demo"
MESSAGE_COUNT = 2


def main() -> None:
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "group.id": GROUP_ID,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([TOPIC])
    received = 0
    try:
        while received < MESSAGE_COUNT:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())
            key = (msg.key() or b"").decode("utf-8")
            payload = json.loads((msg.value() or b"{}").decode("utf-8"))
            print(
                f"{key}, {payload['line_id']}, "
                f"{payload['time_to_station_sec']}, {payload['obs_ts']}"
            )
            received += 1
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
