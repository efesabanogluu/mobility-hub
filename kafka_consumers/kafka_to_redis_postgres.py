import json
from kafka import KafkaConsumer
import redis
import psycopg2
import time
from kafka.errors import NoBrokersAvailable

TOPICS = [
    'enriched.driver',
    'enriched.passenger',
    'enriched.vehicle_type',
    'enriched.h3'
]

MAX_RETRIES = 10
WAIT_SECONDS = 5

for attempt in range(MAX_RETRIES):
    try:
        consumer = KafkaConsumer(
            *TOPICS,
            bootstrap_servers='kafka:29092',
            auto_offset_reset='earliest',
            group_id='data-writer-group',
            value_deserializer=lambda m: m.decode('utf-8')
        )
        print("✅ Kafka bağlantısı başarılı")
        break
    except NoBrokersAvailable:
        print(f"⏳ Kafka broker henüz hazır değil. Tekrar denenecek... ({attempt+1}/{MAX_RETRIES})")
        time.sleep(WAIT_SECONDS)
else:
    print("❌ Kafka'ya bağlanılamadı. Uygulama kapatılıyor.")
    exit(1)

# Redis
redis_client = redis.Redis(host='redis', port=6379, decode_responses=True)

# PostgreSQL
pg_conn = psycopg2.connect(
    dbname='mobility',
    user='postgres',
    password='postgres',
    host='postgres',
    port=5432
)
pg_cursor = pg_conn.cursor()

# Tabloları oluştur
pg_cursor.execute("""
CREATE TABLE IF NOT EXISTS driver_metrics (
    id SERIAL PRIMARY KEY,
    driver_id TEXT,
    duration_min FLOAT,
    distance_km FLOAT,
    price FLOAT
);
CREATE TABLE IF NOT EXISTS passenger_metrics (
    id SERIAL PRIMARY KEY,
    passenger_id TEXT,
    duration_min FLOAT,
    distance_km FLOAT,
    price FLOAT
);
CREATE TABLE IF NOT EXISTS vehicle_type_metrics (
    id SERIAL PRIMARY KEY,
    vehicle_type TEXT,
    duration_min FLOAT,
    distance_km FLOAT,
    price FLOAT
);
CREATE TABLE IF NOT EXISTS h3_metrics (
    id SERIAL PRIMARY KEY,
    h3_region TEXT,
    duration_min FLOAT,
    distance_km FLOAT,
    price FLOAT
);
""")
pg_conn.commit()

# PostgreSQL yazma fonksiyonları
def write_driver(data):
    pg_cursor.execute("""
        INSERT INTO driver_metrics (driver_id, duration_min, distance_km, price)
        VALUES (%s, %s, %s, %s)
    """, (
        data.get("driver_id"),
        data.get("duration_min", 0),
        data.get("distance_km", 0),
        data.get("price", 0)
    ))
    pg_conn.commit()

def write_passenger(data):
    pg_cursor.execute("""
        INSERT INTO passenger_metrics (passenger_id, duration_min, distance_km, price)
        VALUES (%s, %s, %s, %s)
    """, (
        data.get("passenger_id"),
        data.get("duration_min", 0),
        data.get("distance_km", 0),
        data.get("price", 0)
    ))
    pg_conn.commit()

def write_vehicle_type(data):
    pg_cursor.execute("""
        INSERT INTO vehicle_type_metrics (vehicle_type, duration_min, distance_km, price)
        VALUES (%s, %s, %s, %s)
    """, (
        data.get("vehicle_type"),
        data.get("duration_min", 0),
        data.get("distance_km", 0),
        data.get("price", 0)
    ))
    pg_conn.commit()

def write_h3(data):
    pg_cursor.execute("""
        INSERT INTO h3_metrics (h3_region, duration_min, distance_km, price)
        VALUES (%s, %s, %s, %s)
    """, (
        data.get("h3_region"),
        data.get("duration_min", 0),
        data.get("distance_km", 0),
        data.get("price", 0)
    ))
    pg_conn.commit()

print("🚀 Kafka dinleniyor...")

# Kafka mesajlarını işle
for msg in consumer:
    try:
        topic = msg.topic
        data = json.loads(msg.value)

        if topic == "enriched.driver":
            key = f"driver:{data['driver_id']}"
            redis_client.set(key, json.dumps(data), ex=300)
            write_driver(data)

        elif topic == "enriched.passenger":
            key = f"passenger:{data['passenger_id']}"
            redis_client.set(key, json.dumps(data), ex=300)
            write_passenger(data)

        elif topic == "enriched.vehicle_type":
            key = f"vehicle_type:{data['vehicle_type']}"
            redis_client.set(key, json.dumps(data), ex=300)
            write_vehicle_type(data)

        elif topic == "enriched.h3":
            key = f"h3:{data['h3_region']}"
            redis_client.set(key, json.dumps(data), ex=300)
            write_h3(data)

        print(f"✅ [{topic}] işlendi: {key}")

    except Exception as e:
        print("⛔ Hata:", e)
