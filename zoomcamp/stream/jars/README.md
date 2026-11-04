# Flink connector JARs

Download the Kafka SQL connector (Flink 2.2 line) before running `print_arrivals_topic.py`:

```bash
curl -L -o stream/jars/flink-sql-connector-kafka-5.0.0-2.2.jar \
  https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/5.0.0-2.2/flink-sql-connector-kafka-5.0.0-2.2.jar
```

The binary is not committed (large). Maven coordinates: `org.apache.flink:flink-sql-connector-kafka:5.0.0-2.2`.
