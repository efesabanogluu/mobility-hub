from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.window import TumblingProcessingTimeWindows
from pyflink.common import Types, Time
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema
import helpers

def main():
    print("H3 Metrics Job Started")

    env = StreamExecutionEnvironment.get_execution_environment()

    # Use two parallel operator instances for processing
    env.set_parallelism(2)

    # Enable checkpointing for fault tolerance (5 second interval)
    env.enable_checkpointing(5000)

    # Kafka configuration for source: topic, deserialization, consumer group
    kafka_props = {
        'bootstrap.servers': 'kafka:29092',
        'group.id': 'flink-h3-metrics',
        'auto.offset.reset': 'earliest'  # Reprocess from beginning if needed
    }

    # Kafka source: consume from enriched.h3 topic (contains location-based enriched events)
    consumer = FlinkKafkaConsumer(
        topics='enriched.h3',
        deserialization_schema=SimpleStringSchema(),
        properties=kafka_props
    )

    stream = env.add_source(consumer)

    # Parse each incoming message into a structured Row object
    # Row: [h3_region, duration_min, distance_km, price, count, start_time]
    enriched = stream.map(
        lambda raw: helpers.parse_enriched_json(raw, "h3_region"),
        output_type=Types.ROW([
            Types.STRING(),  # h3_region (derived from start_location_id)
            Types.FLOAT(),   # duration
            Types.FLOAT(),   # distance
            Types.FLOAT(),   # price
            Types.FLOAT(),   # count (always 1.0 for aggregation)
            Types.STRING()   # start_time (optional but retained for potential ordering)
        ])
    )

    # Apply 5-minute tumbling window aggregations:
    # - Group by h3_region
    # - Reduce with custom aggregation logic (sum + count)
    # - Format output as JSON string for downstream consumers
    aggregated = enriched \
        .key_by(lambda x: x[0]) \
        .window(TumblingProcessingTimeWindows.of(Time.minutes(5))) \
        .reduce(helpers.AggregateMetrics()) \
        .map(helpers.format_h3, output_type=Types.STRING())

    # Kafka sink: write aggregated metrics to aggregated.h3 topic
    producer = FlinkKafkaProducer(
        topic='aggregated.h3',
        serialization_schema=SimpleStringSchema(),
        producer_config={'bootstrap.servers': 'kafka:29092'}
    )

    aggregated.add_sink(producer)

    env.execute("H3 Region Metrics Aggregation")

if __name__ == "__main__":
    main()
