#!/bin/bash

KAFKA_CONTAINER=mobility-hub-kafka-1
BROKER=kafka:29092

# Number of partitions per topic
PARTITIONS=3

# Replication factor (set to 1 for local dev setup)
REPLICATION=1

# Max number of retry attempts per topic
MAX_RETRIES=15

# List of topics to be created (raw, enriched, and aggregated layers)
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

# Function to create a Kafka topic with retry mechanism
create_topic_with_retry() {
  local topic=$1
  local attempt=1

  while true; do
    echo "⏳ Attempt $attempt: Creating topic '$topic'..."

    # Run kafka-topics command inside the Kafka container to create the topic
    docker exec -i "$KAFKA_CONTAINER" kafka-topics \
      --bootstrap-server "$BROKER" \
      --create \
      --if-not-exists \
      --topic "$topic" \
      --partitions "$PARTITIONS" \
      --replication-factor "$REPLICATION" && break

    echo "⚠️ Topic '$topic' creation failed. Retrying in 2s..."
    attempt=$((attempt+1))

    if [ "$attempt" -gt "$MAX_RETRIES" ]; then
      echo "❌ Failed to create topic '$topic' after $MAX_RETRIES attempts."
      return 1
    fi

    sleep 2
  done

  echo "✅ Topic '$topic' created or already exists."
}

# Loop through all topics and create them one by one
for TOPIC in "${TOPICS[@]}"; do
  create_topic_with_retry "$TOPIC"
done

echo "✅ Topic creation completed."
