"""Read tfl.arrivals from Redpanda once and print JSON values (bounded Kafka source)."""

from __future__ import annotations

import sys
from pathlib import Path

STREAM_DIR = Path(__file__).resolve().parent
ZOOMCAMP_ROOT = STREAM_DIR.parent
if str(ZOOMCAMP_ROOT) not in sys.path:
    sys.path.insert(0, str(ZOOMCAMP_ROOT))

from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource

KAFKA_CONNECTOR_JAR = (
    STREAM_DIR / "jars" / "flink-sql-connector-kafka-5.0.0-2.2.jar"
)
BOOTSTRAP = "127.0.0.1:19092"
TOPIC = "tfl.arrivals"
GROUP_ID = "transitpulse-flink-print"


def main() -> None:
    if not KAFKA_CONNECTOR_JAR.is_file():
        raise FileNotFoundError(
            f"Missing Kafka connector JAR: {KAFKA_CONNECTOR_JAR}"
        )

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.add_jars(KAFKA_CONNECTOR_JAR.as_uri())

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(BOOTSTRAP)
        .set_topics(TOPIC)
        .set_group_id(GROUP_ID)
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_bounded(KafkaOffsetsInitializer.latest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    env.from_source(
        source,
        WatermarkStrategy.no_watermarks(),
        "kafka-arrivals",
    ).print()
    env.execute("print_arrivals_topic")


if __name__ == "__main__":
    main()
