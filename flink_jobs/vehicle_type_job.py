from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.window import TumblingProcessingTimeWindows
from pyflink.common import Types, Time
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema
import helpers


def main():
    print("Vehicle Type Metrics Job Started")
    env = StreamExecutionEnvironment.get_execution_environment()

    # Run 2 parallel operator instances
    env.set_parallelism(2)

    # Enable checkpointing every 5 seconds for fault-tolerance and recovery
    env.enable_checkpointing(5000)

    # Kafka configuration for the Flink job's consumer group
    kafka_props = {
        'bootstrap.servers': 'kafka:29092',
        'group.id': 'flink-vehicle-type',
        'auto.offset.reset': 'earliest'  # start from beginning of topic if no committed offsets exist
    }

    # Create Kafka source to consume enriched events grouped by vehicle_type
    consumer = FlinkKafkaConsumer(
        topics='enriched.vehicle_type',
        deserialization_schema=SimpleStringSchema(),
        properties=kafka_props
    )

    stream = env.add_source(consumer)

    # Convert JSON string to typed Row for Flink processing
    # Row format: [vehicle_type, duration_min, distance_km, price, count, start_time]
    enriched = stream.map(
        lambda raw: helpers.parse_enriched_json(raw, "vehicle_type"),
        output_type=Types.ROW([
            Types.STRING(),  # vehicle_type
            Types.FLOAT(),   # duration_min
            Types.FLOAT(),   # distance_km
            Types.FLOAT(),   # price
            Types.FLOAT(),   # count (always 1.0 initially)
            Types.STRING()   # start_time (can be used for ordering or later features)
        ])
    )

    # Apply 5-minute tumbling windows per vehicle_type
    # Aggregate duration, distance, price, and count using custom ReduceFunction
    aggregated = enriched \
        .key_by(lambda x: x[0]) \
        .window(TumblingProcessingTimeWindows.of(Time.minutes(5))) \
        .reduce(helpers.AggregateMetrics()) \
        .map(helpers.format_vehicle_type, output_type=Types.STRING())

    # Kafka sink: write aggregated metrics to 'aggregated.vehicle_type' topic
    producer = FlinkKafkaProducer(
        topic='aggregated.vehicle_type',
        serialization_schema=SimpleStringSchema(),
        producer_config={'bootstrap.servers': 'kafka:29092'}
    )

    aggregated.add_sink(producer)

    env.execute("Vehicle Type Metrics Aggregation")

if __name__ == "__main__":
    main()
