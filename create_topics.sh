#!/bin/bash

KAFKA_CONTAINER=kafka
BROKER=localhost:9092
PARTITIONS=6
REPLICATION=1

TOPICS=(
  trips_stream
  enriched.driver
  enriched.passenger
  enriched.vehicle_type
  enriched.h3
  aggregated.driver
  aggregated.passenger
  aggregated.vehicle_type
  aggregated.h3
)

echo "🔧 Creating Kafka topics (partition: $PARTITIONS)..."

for TOPIC in "${TOPICS[@]}"
do
  echo "⏳ Creating topic: $TOPIC"
  docker exec -it $KAFKA_CONTAINER kafka-topics \
    --bootstrap-server $BROKER \
    --create \
    --if-not-exists \
    --topic "$TOPIC" \
    --partitions $PARTITIONS \
    --replication-factor $REPLICATION
done

echo "✅ Topic creation completed."
