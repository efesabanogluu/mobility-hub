import json
import time
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
import redis
import psycopg2

# Topics containing aggregated metrics from Flink
TOPICS = [
    'aggregated.driver',
    'aggregated.passenger',
    'aggregated.vehicle_type',
    'aggregated.h3'
]

# Kafka retry strategy
MAX_RETRIES = 10
WAIT_SECONDS = 5

# Attempt Kafka connection with retries
for attempt in range(MAX_RETRIES):
    try:
        consumer = KafkaConsumer(
            *TOPICS,
            bootstrap_servers='kafka:29092',
            auto_offset_reset='earliest',
            group_id='data-writer-group',
            value_deserializer=lambda m: m.decode('utf-8')
        )
        print("✅ Kafka connection established")
        break
    except NoBrokersAvailable:
        print(f"⏳ Kafka broker not ready yet. Retrying... ({attempt+1}/{MAX_RETRIES})")
        time.sleep(WAIT_SECONDS)
else:
    print("❌ Failed to connect to Kafka. Shutting down.")
    exit(1)

# Redis
redis_client = redis.Redis(host='redis', port=6379, decode_responses=True)

# Connect to PostgreSQL for long-term storage of aggregated metrics
pg_conn = psycopg2.connect(
    dbname='mobility',
    user='postgres',
    password='postgres',
    host='postgres',
    port=5432
)
pg_cursor = pg_conn.cursor()

# Create necessary tables if they don't exist
pg_cursor.execute("""
CREATE TABLE IF NOT EXISTS driver_metrics (
    driver_id TEXT PRIMARY KEY,
    trip_count INT,
    avg_duration FLOAT,
    total_distance FLOAT,
    total_revenue FLOAT
);
CREATE TABLE IF NOT EXISTS passenger_metrics (
    passenger_id TEXT PRIMARY KEY,
    trip_count INT,
    avg_duration FLOAT,
    total_distance FLOAT,
    total_revenue FLOAT
);
CREATE TABLE IF NOT EXISTS vehicle_type_metrics (
    vehicle_type TEXT PRIMARY KEY,
    trip_count INT,
    avg_duration FLOAT,
    total_distance FLOAT,
    total_revenue FLOAT
);
CREATE TABLE IF NOT EXISTS h3_metrics (
    h3_region TEXT PRIMARY KEY,
    trip_count INT,
    avg_duration FLOAT,
    total_distance FLOAT,
    total_revenue FLOAT
);
""")
pg_conn.commit()

# Redis update function: merges existing and new metrics using weighted averages
def update_redis(key, new_data):
    existing = redis_client.get(key)
    if existing:
        old = json.loads(existing)
        total_count = old["trip_count"] + new_data["trip_count"]
        avg_duration = (
            (old["avg_duration"] * old["trip_count"] + new_data["avg_duration"] * new_data["trip_count"])
            / total_count
        ) if total_count > 0 else 0

        updated = {
            "trip_count": total_count,
            "total_distance": old["total_distance"] + new_data["total_distance"],
            "total_revenue": old["total_revenue"] + new_data["total_revenue"],
            "avg_duration": avg_duration
        }

        # Retain entity key (e.g. driver_id, passenger_id, etc.)
        for k in ["driver_id", "passenger_id", "vehicle_type", "h3_region"]:
            if k in new_data:
                updated[k] = new_data[k]

        redis_client.set(key, json.dumps(updated), ex=86400)
    else:
        redis_client.set(key, json.dumps(new_data), ex=86400)

# PostgreSQL UPSERT using ON CONFLICT
def write_entity(table, id_field, data):
    pg_cursor.execute(f"""
        INSERT INTO {table} ({id_field}, trip_count, avg_duration, total_distance, total_revenue)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT ({id_field}) DO UPDATE SET
            trip_count = {table}.trip_count + EXCLUDED.trip_count,
            total_distance = {table}.total_distance + EXCLUDED.total_distance,
            total_revenue = {table}.total_revenue + EXCLUDED.total_revenue,
            avg_duration = (
                ({table}.avg_duration * {table}.trip_count + EXCLUDED.avg_duration * EXCLUDED.trip_count)
                / NULLIF({table}.trip_count + EXCLUDED.trip_count, 0)
            );
    """, (
        data.get(id_field),
        data.get("trip_count", 0),
        data.get("avg_duration", 0),
        data.get("total_distance", 0),
        data.get("total_revenue", 0)
    ))
    pg_conn.commit()

print("🚀 Listening for Kafka messages...")

# Continuously consume and dispatch incoming Kafka messages
for msg in consumer:
    try:
        topic = msg.topic
        data = json.loads(msg.value)

        if topic == "aggregated.driver":
            key = f"driver:{data['driver_id']}"
            update_redis(key, data)
            write_entity("driver_metrics", "driver_id", data)

        elif topic == "aggregated.passenger":
            key = f"passenger:{data['passenger_id']}"
            update_redis(key, data)
            write_entity("passenger_metrics", "passenger_id", data)

        elif topic == "aggregated.vehicle_type":
            key = f"vehicle_type:{data['vehicle_type']}"
            update_redis(key, data)
            write_entity("vehicle_type_metrics", "vehicle_type", data)

        elif topic == "aggregated.h3":
            key = f"h3:{data['h3_region']}"
            update_redis(key, data)
            write_entity("h3_metrics", "h3_region", data)

        print(f"✅ [{topic}] processed: {key}")

    except Exception as e:
        print("⛔ Error:", e)
