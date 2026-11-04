"""Print frozen Oxford Circus arrivals via PyFlink (in-memory; no Redpanda)."""

from __future__ import annotations

import json
import sys
from pathlib import Path

ZOOMCAMP_ROOT = Path(__file__).resolve().parent.parent
if str(ZOOMCAMP_ROOT) not in sys.path:
    sys.path.insert(0, str(ZOOMCAMP_ROOT))

from pyflink.datastream import StreamExecutionEnvironment

from ingest.tfl_arrivals_producer import poll_arrivals

OXFORD_ARRIVALS = ZOOMCAMP_ROOT / "sample_data" / "arrivals_oxford_circus.json"
OBS_TS = "2026-11-01T17:13:00Z"
OXFORD_STOP_ID = "940GZZLUOXC"


def main() -> None:
    oxford_predictions = json.loads(OXFORD_ARRIVALS.read_text(encoding="utf-8"))

    def fetch(stop_id: str) -> list[dict]:
        if stop_id == OXFORD_STOP_ID:
            return oxford_predictions
        return []

    messages = poll_arrivals(fetch, OBS_TS)
    rows = [
        (
            msg["stop_id"],
            msg["line_id"],
            msg["time_to_station_sec"],
            msg["obs_ts"],
        )
        for msg in messages
    ]

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.from_collection(rows).print()
    env.execute("print_arrivals_sample")


if __name__ == "__main__":
    main()
