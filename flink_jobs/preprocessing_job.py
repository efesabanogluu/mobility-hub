from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.typeinfo import Types
import pandas as pd
import json

# === Read enrichment data ===
driver_df = pd.read_parquet("/opt/data/drivers.parquet")
driver_map = dict(zip(driver_df["driver_id"], driver_df["vehicle_type"]))

# === Enrichment Function ===
def enrich_and_split(event_json):
    event = json.loads(event_json)
    enriched_events = []

    try:
        driver_id = event.get("driver_id", "unknown")
        passenger_id = event.get("passenger_id", "unknown")
        vehicle_type = driver_map.get(driver_id, "unknown")
        h3_region = str(event.get("start_location_id", "unknown"))

        common_metrics = {
            "duration_min": float(event.get("duration_min", 0) or 0),
            "distance_km": float(event.get("distance_km", 0) or 0),
            "price": float(event.get("price", 0) or 0)
        }

        enriched_events.append(("driver", json.dumps({ "driver_id": driver_id, **common_metrics })))
        enriched_events.append(("passenger", json.dumps({ "passenger_id": passenger_id, **common_metrics })))
        enriched_events.append(("vehicle_type", json.dumps({ "vehicle_type": vehicle_type, **common_metrics })))
        enriched_events.append(("h3", json.dumps({
            "h3_region": h3_region,
            **common_metrics
        })))
    except Exception as e:
        print("⛔ Enrichment failed:", e)

    return enriched_events

def main():
    print("Preprocessing Flink job started")
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    kafka_props = {
        'bootstrap.servers': 'kafka:29092',
        'group.id': 'flink-preprocessing',
        'auto.offset.reset': 'earliest'
    }

    consumer = FlinkKafkaConsumer(
        topics='trips_stream',
        deserialization_schema=SimpleStringSchema(),
        properties=kafka_props
    )

    stream = env.add_source(consumer)

    split_stream = stream.flat_map(
        lambda raw: enrich_and_split(raw),
        output_type=Types.TUPLE([Types.STRING(), Types.STRING()])
    )

    for agg_type in ["driver", "passenger", "vehicle_type", "h3"]:
        producer = FlinkKafkaProducer(
            topic=f"enriched.{agg_type}",
            serialization_schema=SimpleStringSchema(),
            producer_config={'bootstrap.servers': 'kafka:29092'}
        )

        split_stream \
            .filter(lambda x: x[0] == agg_type) \
            .map(lambda x: x[1], output_type=Types.STRING()) \
            .add_sink(producer)

    env.execute("Preprocessing and Enrichment")

if __name__ == "__main__":
    main()
