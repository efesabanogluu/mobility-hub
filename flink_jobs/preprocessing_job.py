from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.typeinfo import Types
import pandas as pd
import json

# === Read enrichment data ===
# Load driver metadata (e.g., vehicle type) from Parquet into DataFrame
driver_df = pd.read_parquet("/opt/data/drivers.parquet")
# Map driver_id to vehicle_type for fast lookup during enrichment
driver_map = dict(zip(driver_df["driver_id"], driver_df["vehicle_type"]))

# === Enrichment Function ===
# Enrichment logic: transform a raw trip event into multiple enriched outputs per entity type
def enrich_and_split(event_json):
    event = json.loads(event_json)
    enriched_events = []

    try:
        driver_id = event.get("driver_id", "unknown")
        passenger_id = event.get("passenger_id", "unknown")
        vehicle_type = driver_map.get(driver_id, "unknown")
        # Use start_location_id as a proxy for spatial region (not actual H3 encoding)
        h3_region = str(event.get("start_location_id", "unknown"))
        start_time = event.get("start_time", None)

        # Extract core metrics shared by all aggregations: duration, distance, price
        common_metrics = {
            "start_time": start_time,
            "duration_min": float(event.get("duration_min", 0) or 0),
            "distance_km": float(event.get("distance_km", 0) or 0),
            "price": float(event.get("price", 0) or 0)
        }

        # Create enriched event for each entity type (driver, passenger, etc.) with relevant keys
        enriched_events.append(("driver", json.dumps({ "driver_id": driver_id, **common_metrics })))
        enriched_events.append(("passenger", json.dumps({ "passenger_id": passenger_id, **common_metrics })))
        enriched_events.append(("vehicle_type", json.dumps({ "vehicle_type": vehicle_type, **common_metrics })))
        enriched_events.append(("h3", json.dumps({ "h3_region": h3_region, **common_metrics })))
    except Exception as e:
        print("⛔ Enrichment failed:", e)

    return enriched_events

def main():
    print("Preprocessing Flink job started")

    env = StreamExecutionEnvironment.get_execution_environment()

    # Set Flink's parallelism level — determines how many parallel operator instances run
    env.set_parallelism(4)

    # Kafka consumer properties: which broker to connect to, and consumer group config
    kafka_props = {
        'bootstrap.servers': 'kafka:29092',
        'group.id': 'flink-preprocessing',
        'auto.offset.reset': 'earliest'  # start reading from beginning if no committed offset
    }

    # Define the Kafka consumer that reads raw trip events from 'trips_stream' topic
    consumer = FlinkKafkaConsumer(
        topics='trips_stream',
        deserialization_schema=SimpleStringSchema(),  # raw messages expected as plain strings
        properties=kafka_props
    )

    stream = env.add_source(consumer)

    # Apply the enrichment function to each incoming event
    # This transforms one raw event into multiple enriched events (one per entity type)
    split_stream = stream.flat_map(
        lambda raw: enrich_and_split(raw),
        output_type=Types.TUPLE([Types.STRING(), Types.STRING()])  # (entity_type, json_string)
    )

    # For each aggregation type (driver, passenger, vehicle_type, h3),
    for agg_type in ["driver", "passenger", "vehicle_type", "h3"]:
        # Create a Kafka producer targeting the appropriate enriched topic (e.g., enriched.driver)
        producer = FlinkKafkaProducer(
            topic=f"enriched.{agg_type}",
            serialization_schema=SimpleStringSchema(),
            producer_config={'bootstrap.servers': 'kafka:29092'}
        )

        # Filter for only the current entity type, extract the JSON payload,
        # and send it to the matching enriched Kafka topic
        split_stream \
            .filter(lambda x: x[0] == agg_type) \
            .map(lambda x: x[1], output_type=Types.STRING()) \
            .add_sink(producer)

    env.execute("Preprocessing and Enrichment")

if __name__ == "__main__":
    main()
