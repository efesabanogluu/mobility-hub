from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.window import TumblingProcessingTimeWindows
from pyflink.common import Types, Time
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema
import helpers

def main():
    print("Vehicle Type Metrics Job Started")
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.enable_checkpointing(5000)

    kafka_props = {
        'bootstrap.servers': 'kafka:29092',
        'group.id': 'flink-vehicle-typee-{uuid.uuid4()}',
        'auto.offset.reset': 'earliest'
    }

    consumer = FlinkKafkaConsumer(
        topics='enriched.vehicle_type',
        deserialization_schema=SimpleStringSchema(),
        properties=kafka_props
    )

    stream = env.add_source(consumer)
    print("Vehicle Type Metrics 1")

    enriched = stream.map(
        lambda raw: helpers.parse_enriched_json(raw, "vehicle_type"),
        output_type=Types.ROW([
            Types.STRING(),  # vehicle_type
            Types.FLOAT(),
            Types.FLOAT(),
            Types.FLOAT(),
            Types.FLOAT()
        ])
    )
    print("Vehicle Type Metrics 2")

    aggregated = enriched \
        .key_by(lambda x: x[0]) \
        .window(TumblingProcessingTimeWindows.of(Time.seconds(10))) \
        .reduce(helpers.AggregateMetrics()) \
        .map(helpers.format_vehicle_type, output_type=Types.STRING())
    print("Vehicle Type Metrics 3")

    output_path = "/opt/output/vehicle_type_metrics/vehicle_type_metrics.json"
    write_fn = helpers.write_json_to_file(output_path)
    print("Vehicle Type Metrics 4")

    aggregated.map(lambda val: write_fn(val))
    print("Vehicle Type Metrics 5")

    env.execute("Vehicle Type Metrics Aggregation")

if __name__ == "__main__":
    main()
