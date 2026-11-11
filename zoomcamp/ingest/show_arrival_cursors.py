"""Print tfl.arrivals high watermark and committed offsets for live and append groups."""

from __future__ import annotations

import sys
from pathlib import Path

from confluent_kafka import OFFSET_INVALID, Consumer, TopicPartition

ZOOMCAMP_ROOT = Path(__file__).resolve().parent.parent
if str(ZOOMCAMP_ROOT) not in sys.path:
    sys.path.insert(0, str(ZOOMCAMP_ROOT))

from ingest.tfl_arrivals_producer import TOPIC

BOOTSTRAP = "127.0.0.1:19092"
PARTITION = 0
GROUPS = ("transitpulse-arrivals-live", "transitpulse-arrivals-append")


def _committed_offset(group_id: str) -> int | str:
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "group.id": group_id,
            "enable.auto.commit": False,
        }
    )
    partition = TopicPartition(TOPIC, PARTITION)
    try:
        committed = consumer.committed([partition], timeout=10.0)[0]
        if committed.offset == OFFSET_INVALID:
            return "none"
        return committed.offset
    finally:
        consumer.close()


def main() -> None:
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "group.id": "transitpulse-arrivals-cursor-show",
            "enable.auto.commit": False,
        }
    )
    partition = TopicPartition(TOPIC, PARTITION)
    try:
        _low, high = consumer.get_watermark_offsets(partition, timeout=10.0)
        print(f"high {high}")
    finally:
        consumer.close()

    for group_id in GROUPS:
        offset = _committed_offset(group_id)
        print(f"{group_id} {offset}")


if __name__ == "__main__":
    main()
