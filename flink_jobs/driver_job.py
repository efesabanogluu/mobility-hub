from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.window import TumblingProcessingTimeWindows
from pyflink.common import Types, Time
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema
import helpers

def main():
    print("Driver Metrics Job Started")
    env = StreamExecutionEnvironment.get_execution_environment()
    
    # Set number of parallel operator instances
    env.set_parallelism(2)

    # Enable checkpointing every 5 seconds for fault-tolerance and recovery
    env.enable_checkpointing(5000)

    # Kafka connection and consumer group configuration
    kafka_props = {
        'bootstrap.servers': 'kafka:29092',
        'group.id': 'flink-driver-metrics',
        'auto.offset.reset': 'earliest'  # Start from beginning if no offset exists
    }

    # Create Kafka source to read enriched events for drivers
    consumer = FlinkKafkaConsumer(
        topics='enriched.driver',
        deserialization_schema=SimpleStringSchema(),
        properties=kafka_props
    )

    stream = env.add_source(consumer)

    # Parse incoming JSON and convert to typed row:
    # Fields: driver_id, duration, distance, price, count (1), start_time
    enriched = stream.map(
        lambda raw: helpers.parse_enriched_json(raw, "driver_id"),
        output_type=Types.ROW([
            Types.STRING(),  # driver_id
            Types.FLOAT(),   # duration_min
            Types.FLOAT(),   # distance_km
            Types.FLOAT(),   # price
            Types.FLOAT(),   # count (set to 1.0 initially)
            Types.STRING()   # start_time
        ])
    )

    # Windowed aggregation:
    # - Key by driver_id
    # - Group events into 5-minute tumbling windows
    # - Aggregate using custom reducer (sum metrics and count)
    aggregated = enriched \
        .key_by(lambda x: x[0]) \
        .window(TumblingProcessingTimeWindows.of(Time.minutes(5))) \
        .reduce(helpers.AggregateMetrics()) \
        .map(helpers.format_driver, output_type=Types.STRING())

    # Define Kafka producer to publish aggregated driver metrics
    producer = FlinkKafkaProducer(
        topic='aggregated.driver',
        serialization_schema=SimpleStringSchema(),
        producer_config={'bootstrap.servers': 'kafka:29092'}
    )

    aggregated.add_sink(producer)

    env.execute("Driver Metrics Aggregation")


if __name__ == "__main__":
    main()
