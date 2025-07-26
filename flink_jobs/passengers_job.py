from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.window import TumblingProcessingTimeWindows
from pyflink.common import Types, Time
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema
import helpers

def main():
    print("Passenger Metrics Job Started")
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.enable_checkpointing(5000)

    kafka_props = {
        'bootstrap.servers': 'kafka:29092',
        'group.id': 'flink-passenger-metricse-{uuid.uuid4()}',
        'auto.offset.reset': 'earliest'
    }

    consumer = FlinkKafkaConsumer(
        topics='enriched.passenger',
        deserialization_schema=SimpleStringSchema(),
        properties=kafka_props
    )

    stream = env.add_source(consumer)

    enriched = stream.map(
        lambda raw: helpers.parse_enriched_json(raw, "passenger_id"),
        output_type=Types.ROW([
            Types.STRING(),  # passenger_id
            Types.FLOAT(),
            Types.FLOAT(),
            Types.FLOAT(),
            Types.FLOAT()
        ])
    )

    aggregated = enriched \
        .key_by(lambda x: x[0]) \
        .window(TumblingProcessingTimeWindows.of(Time.seconds(10))) \
        .reduce(helpers.AggregateMetrics()) \
        .map(helpers.format_passenger, output_type=Types.STRING())

    producer = FlinkKafkaProducer(
        topic='aggregated.passenger',
        serialization_schema=SimpleStringSchema(),
        producer_config={'bootstrap.servers': 'kafka:29092'}
    )

    aggregated.add_sink(producer)

    env.execute("Passenger Metrics Aggregation")

if __name__ == "__main__":
    main()
