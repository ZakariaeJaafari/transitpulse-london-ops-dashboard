"""Consume new tfl.arrivals records for transitpulse-arrivals-live and commit cursor (host script)."""

from __future__ import annotations

import json
import sys
from collections import Counter
from pathlib import Path

from confluent_kafka import (
    OFFSET_INVALID,
    Consumer,
    KafkaError,
    KafkaException,
    TopicPartition,
)

ZOOMCAMP_ROOT = Path(__file__).resolve().parent.parent
if str(ZOOMCAMP_ROOT) not in sys.path:
    sys.path.insert(0, str(ZOOMCAMP_ROOT))

from ingest.tfl_arrivals_producer import TOPIC

BOOTSTRAP = "127.0.0.1:19092"
GROUP_ID = "transitpulse-arrivals-live"
PARTITION = 0


def main() -> None:
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "group.id": GROUP_ID,
            "enable.auto.commit": False,
        }
    )
    partition = TopicPartition(TOPIC, PARTITION)
    try:
        low, high = consumer.get_watermark_offsets(partition, timeout=10.0)
        if low < 0 or high < 0:
            print(f"watermark low {low} high {high}")
            print("total 0")
            return

        committed = consumer.committed([partition], timeout=10.0)[0]
        if committed.offset == OFFSET_INVALID:
            start = low
        else:
            start = committed.offset

        if start >= high:
            print(f"start {start} high {high}")
            print("total 0")
            return

        consumer.assign([TopicPartition(TOPIC, PARTITION, start)])
        counts: Counter[str] = Counter()
        remaining = high - start
        last_msg = None

        while remaining > 0:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    break
                raise KafkaException(msg.error())

            payload = json.loads((msg.value() or b"{}").decode("utf-8"))
            obs_ts = payload["obs_ts"]
            counts[obs_ts] += 1
            last_msg = msg
            remaining -= 1

        total = sum(counts.values())
        for obs_ts in sorted(counts):
            print(f"{obs_ts} {counts[obs_ts]}")
        print(f"total {total}")

        if last_msg is None:
            return

        consumer.commit(message=last_msg, asynchronous=False)
        next_offset = last_msg.offset() + 1
        print(f"committed through {next_offset}")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
