"""Follow new tfl.arrivals polls; commit after each quiet batch (host script)."""

from __future__ import annotations

import json
import sys
from collections import Counter
from pathlib import Path

from confluent_kafka import Consumer, KafkaError, KafkaException, Message

ZOOMCAMP_ROOT = Path(__file__).resolve().parent.parent
if str(ZOOMCAMP_ROOT) not in sys.path:
    sys.path.insert(0, str(ZOOMCAMP_ROOT))

from ingest.tfl_arrivals_producer import TOPIC

BOOTSTRAP = "127.0.0.1:19092"
GROUP_ID = "transitpulse-arrivals-live"


def _flush_batch(consumer: Consumer, counts: Counter[str], last_msg: Message) -> None:
    total = sum(counts.values())
    for obs_ts in sorted(counts):
        print(f"{obs_ts} {counts[obs_ts]}")
    print(f"total {total}")
    consumer.commit(message=last_msg, asynchronous=False)
    next_offset = last_msg.offset() + 1
    print(f"committed through {next_offset}")


def main() -> None:
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "group.id": GROUP_ID,
            "auto.offset.reset": "latest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([TOPIC])
    print("waiting")

    counts: Counter[str] = Counter()
    last_msg: Message | None = None
    batch_open = False

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                if batch_open and last_msg is not None:
                    _flush_batch(consumer, counts, last_msg)
                    counts.clear()
                    last_msg = None
                    batch_open = False
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())

            payload = json.loads((msg.value() or b"{}").decode("utf-8"))
            obs_ts = payload["obs_ts"]
            counts[obs_ts] += 1
            last_msg = msg
            batch_open = True
    except KeyboardInterrupt:
        if batch_open and last_msg is not None:
            _flush_batch(consumer, counts, last_msg)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
