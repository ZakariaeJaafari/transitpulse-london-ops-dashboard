"""Aggregate tfl.arrivals by line_id in a bounded batch Flink SQL job."""

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

CREATE_ARRIVALS_TABLE = """
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

AGGREGATE_BY_LINE = """
SELECT
  line_id,
  COUNT(*) AS n,
  AVG(time_to_station_sec) AS avg_predicted_seconds
FROM arrivals
GROUP BY line_id
"""


def main() -> None:
    if not KAFKA_CONNECTOR_JAR.is_file():
        raise FileNotFoundError(
            f"Missing Kafka connector JAR: {KAFKA_CONNECTOR_JAR}"
        )

    t_env = TableEnvironment.create(EnvironmentSettings.in_batch_mode())
    t_env.get_config().set("pipeline.jars", KAFKA_CONNECTOR_JAR.as_uri())

    t_env.execute_sql(CREATE_ARRIVALS_TABLE)
    t_env.execute_sql(AGGREGATE_BY_LINE).print()


if __name__ == "__main__":
    main()
