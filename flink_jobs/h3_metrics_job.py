from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.window import TumblingProcessingTimeWindows
from pyflink.common import Types, Time
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema
import helpers

def main():
    print("H3 Metrics Job Started")
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.enable_checkpointing(5000)

    kafka_props = {
        'bootstrap.servers': 'kafka:29092',
        'group.id': 'flink-h3-metricse-{uuid.uuid4()}',
        'auto.offset.reset': 'earliest'
    }

    consumer = FlinkKafkaConsumer(
        topics='enriched.h3',
        deserialization_schema=SimpleStringSchema(),
        properties=kafka_props
    )

    stream = env.add_source(consumer)

    enriched = stream.map(
        lambda raw: helpers.parse_enriched_json(raw, "h3_region"),
        output_type=Types.ROW([
            Types.STRING(),  # h3_region
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
        .map(helpers.format_h3, output_type=Types.STRING())

    output_path = "/opt/output/h3_metrics/h3_metrics.json"
    write_fn = helpers.write_json_to_file(output_path)

    aggregated.map(lambda val: write_fn(val))

    env.execute("H3 Region Metrics Aggregation")

if __name__ == "__main__":
    main()
