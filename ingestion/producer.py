# Read static batch data from Parquet
import pandas as pd
import json
import time
from kafka import KafkaProducer

# Kafka producer configuration:
# - Connect to local broker
# - Serialize payloads as UTF-8 encoded JSON
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

df = pd.read_parquet('data/trips.parquet')

# Sort trips by start_time to simulate temporal ordering
df = df.sort_values('start_time')
df['start_time'] = pd.to_datetime(df['start_time'])

# Define simulation time window size (in minutes)
chunk_minutes = 5
start_time = df['start_time'].min()
end_time = df['start_time'].max()
current_time = start_time

# Simulate real-time event streaming: step forward in 5-minute increments
while current_time < end_time:
    next_time = current_time + pd.Timedelta(minutes=chunk_minutes)
    chunk = df[(df['start_time'] >= current_time) & (df['start_time'] < next_time)]
    try:
        print(chunk.iloc[0].to_dict())
    except IndexError:
        print(f"⚠️ Skipping empty chunk {next_time} - {current_time}")

    # Send each row as a JSON message to Kafka topic 'trips_stream'
    for _, row in chunk.iterrows():
        payload = row.to_dict()
        for k, v in payload.items():
            if isinstance(v, pd.Timestamp):
                payload[k] = v.isoformat()

        producer.send("trips_stream", value=payload)

    print(f"✅ Sent {len(chunk)} records from {current_time} to {next_time}")

    # Force Kafka to send all buffered messages immediately
    producer.flush()

    # Simulate delay between each batch to mimic real-time ingestion
    time.sleep(2)  # 2-second delay per batch window

    current_time = next_time

producer.close()
