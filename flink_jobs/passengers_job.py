from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.window import TumblingProcessingTimeWindows
from pyflink.common import Types, Time
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema
import helpers

def main():
    print("Passenger Metrics Job Started")

    env = StreamExecutionEnvironment.get_execution_environment()

    # Set operator parallelism
    env.set_parallelism(2)

    # Enable state checkpointing for recovery and fault-tolerance
    env.enable_checkpointing(5000)

    # Kafka configuration properties for Flink consumer group
    kafka_props = {
        'bootstrap.servers': 'kafka:29092',
        'group.id': 'flink-passenger-metrics',
        'auto.offset.reset': 'earliest'
    }

    # Consume enriched events from Kafka topic: enriched.passenger
    consumer = FlinkKafkaConsumer(
        topics='enriched.passenger',
        deserialization_schema=SimpleStringSchema(),
        properties=kafka_props
    )

    stream = env.add_source(consumer)

    # Parse JSON payload and convert to Flink Row type for processing
    enriched = stream.map(
        lambda raw: helpers.parse_enriched_json(raw, "passenger_id"),
        output_type=Types.ROW([
            Types.STRING(),  # passenger_id
            Types.FLOAT(),
            Types.FLOAT(),
            Types.FLOAT(),
            Types.FLOAT(),
            Types.STRING()
        ])
    )

    # Partition stream by passenger identifier
    # Apply 5-minute tumbling windows for aggregation
    # Reduce windowed elements using custom aggregation logic
    # Format aggregated results to JSON string for Kafka output
    aggregated = enriched \
        .key_by(lambda x: x[0]) \
        .window(TumblingProcessingTimeWindows.of(Time.minutes(5))) \
        .reduce(helpers.AggregateMetrics()) \
        .map(helpers.format_passenger, output_type=Types.STRING())

    # Send aggregated passenger metrics to Kafka topic: aggregated.passenger
    producer = FlinkKafkaProducer(
        topic='aggregated.passenger',
        serialization_schema=SimpleStringSchema(),
        producer_config={'bootstrap.servers': 'kafka:29092'}
    )

    aggregated.add_sink(producer)

    env.execute("Passenger Metrics Aggregation")

if __name__ == "__main__":
    main()
