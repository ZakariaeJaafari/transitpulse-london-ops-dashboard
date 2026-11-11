"""Aggregate tfl.arrivals by line_id and write JSON to the local filesystem."""

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
OUTPUT_DIR = STREAM_DIR / "output" / "arrivals_by_line"


def create_arrivals_table() -> str:
    return """
CREATE TABLE arrivals (
  stop_id STRING,
  line_id STRING,
  time_to_station_sec INT,
  obs_ts STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'tfl.arrivals',
  'properties.bootstrap.servers' = '127.0.0.1:19092',
  'properties.group.id' = 'transitpulse-flink-by-line',
  'scan.startup.mode' = 'earliest-offset',
  'scan.bounded.mode' = 'latest-offset',
  'format' = 'json'
)
"""


def create_arrivals_by_line_table(output_uri: str) -> str:
    return f"""
CREATE TABLE arrivals_by_line (
  line_id STRING,
  n BIGINT,
  avg_predicted_seconds DOUBLE
) WITH (
  'connector' = 'filesystem',
  'path' = '{output_uri}',
  'format' = 'json'
)
"""


INSERT_AGGREGATE = """
INSERT INTO arrivals_by_line
SELECT
  line_id,
  CAST(COUNT(*) AS BIGINT),
  CAST(AVG(time_to_station_sec) AS DOUBLE)
FROM arrivals
GROUP BY line_id
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
            "this job always reads from the earliest offset — delete the folder or use a "
            "new path before re-running.",
            file=sys.stderr,
        )
        sys.exit(1)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_uri = OUTPUT_DIR.as_uri()

    t_env = TableEnvironment.create(EnvironmentSettings.in_batch_mode())
    t_env.get_config().set("pipeline.jars", KAFKA_CONNECTOR_JAR.as_uri())

    t_env.execute_sql(create_arrivals_table())
    t_env.execute_sql(create_arrivals_by_line_table(output_uri))
    t_env.execute_sql(INSERT_AGGREGATE).wait()


if __name__ == "__main__":
    main()
