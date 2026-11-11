"""Read tfl.arrivals and write one JSON object per prediction to the filesystem."""

from __future__ import annotations

import sys
from pathlib import Path

STREAM_DIR = Path(__file__).resolve().parent
ZOOMCAMP_ROOT = STREAM_DIR.parent
if str(ZOOMCAMP_ROOT) not in sys.path:
    sys.path.insert(0, str(ZOOMCAMP_ROOT))

from pyflink.table import EnvironmentSettings, TableEnvironment

KAFKA_CONNECTOR_JAR = (
    STREAM_DIR / "jars" / "flink-sql-connector-kafka-5.0.0-2.2.jar"
)
OUTPUT_DIR = STREAM_DIR / "output" / "arrival_updates"


def create_arrivals_table() -> str:
    return """
CREATE TABLE arrivals (
  stop_id STRING,
  line_id STRING,
  vehicle_id STRING,
  expected_arrival STRING,
  time_to_station_sec INT,
  towards STRING,
  obs_ts STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'tfl.arrivals',
  'properties.bootstrap.servers' = '127.0.0.1:19092',
  'properties.group.id' = 'transitpulse-flink-arrival-updates',
  'scan.startup.mode' = 'earliest-offset',
  'scan.bounded.mode' = 'latest-offset',
  'format' = 'json'
)
"""


def create_arrival_updates_table(output_uri: str) -> str:
    return f"""
CREATE TABLE arrival_updates (
  stop_id STRING,
  line_id STRING,
  vehicle_id STRING,
  expected_arrival STRING,
  time_to_station_sec INT,
  towards STRING,
  obs_ts STRING
) WITH (
  'connector' = 'filesystem',
  'path' = '{output_uri}',
  'format' = 'json'
)
"""


INSERT_UPDATES = """
INSERT INTO arrival_updates
SELECT
  stop_id,
  line_id,
  vehicle_id,
  expected_arrival,
  time_to_station_sec,
  towards,
  obs_ts
FROM arrivals
"""


def main() -> None:
    if not KAFKA_CONNECTOR_JAR.is_file():
        raise FileNotFoundError(
            f"Missing Kafka connector JAR: {KAFKA_CONNECTOR_JAR}"
        )

    existing_parts = list(OUTPUT_DIR.rglob("part-*"))
    if existing_parts:
        print(
            f"Error: output already has {len(existing_parts)} part file(s) under "
            f"{OUTPUT_DIR.relative_to(ZOOMCAMP_ROOT)}; "
            "this job always reads from earliest offset — delete the folder or use a "
            "new path before re-running.",
            file=sys.stderr,
        )
        sys.exit(1)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_uri = OUTPUT_DIR.as_uri()

    t_env = TableEnvironment.create(EnvironmentSettings.in_batch_mode())
    t_env.get_config().set("pipeline.jars", KAFKA_CONNECTOR_JAR.as_uri())

    t_env.execute_sql(create_arrivals_table())
    t_env.execute_sql(create_arrival_updates_table(output_uri))
    t_env.execute_sql(INSERT_UPDATES).wait()


if __name__ == "__main__":
    main()
