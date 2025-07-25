# ingestion/producer.py
import pandas as pd
import json
import time
from kafka import KafkaProducer

# Kafka config
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Parquet dosyasını oku
df = pd.read_parquet('../data/trips.parquet')
df = df.sort_values('start_time')
df['start_time'] = pd.to_datetime(df['start_time'])

# 5 dakikalık simülasyon dilimlerini üret
chunk_minutes = 5
start_time = df['start_time'].min()
end_time = df['start_time'].max()
current_time = start_time

while current_time < end_time:
    next_time = current_time + pd.Timedelta(minutes=chunk_minutes)
    chunk = df[(df['start_time'] >= current_time) & (df['start_time'] < next_time)]
    try:
        print(chunk.iloc[0].to_dict())
    except IndexError:
        print(f"⚠️ Skipping empty chunk {next_time} - {current_time}")

    for _, row in chunk.iterrows():
        payload = row.to_dict()
        # Zaman bilgisi JSON içinde datetime olamaz
        for k, v in payload.items():
            if isinstance(v, pd.Timestamp):
                payload[k] = v.isoformat()
        producer.send("trips_stream", value=payload)

    print(f"✅ Sent {len(chunk)} records from {current_time} to {next_time}")
    producer.flush()
    time.sleep(2)  # gerçek zaman simülasyonu: her batch 2 saniyede

    current_time = next_time

producer.close()
