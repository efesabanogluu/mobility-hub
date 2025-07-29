# Mobility Hub — Real-Time Data Pipeline for Ride-Hailing & Mobility Analytics

A complete real-time data infrastructure for processing and analyzing ride-hailing and micro-mobility (e-scooters, bikes) trips. This project simulates trip events from historical `.parquet` files and processes them through a streaming architecture using **Apache Flink**, **Kafka**, **Redis**, and **Python**.

## 🚀 Features

- Real-time ingestion of trip events from `.parquet` files via Kafka
- Entity-level enrichment and aggregation:
  - Per Driver
  - Per Passenger
  - Per Vehicle Type (car, scooter, bike)
  - Per Location ID (`start_location_id`)
- Aggregated metrics:
  - Total trip count
  - Average duration
  - Total distance traveled
  - Revenue generated
- Modular job structure using Apache Flink
- Aggregated output persisted in Redis and Postgres
- Lightweight API to expose metrics in real-time
- Fully Dockerized stack with `docker-compose`

## 🏗️ Architecture

```
┌──────────────┐      ┌────────────────────────────┐      ┌────────────────────────┐
│ *.parquet    ├─────▶│ Kafka Topic: trips_stream  ├─────▶│     Flink Jobs         │
└──────────────┘      └────────────────────────────┘      │  (enrichment + agg)    │
                                                          └────────────┬───────────┘
                                                                       │
                                                                       ▼                                     
                                                  ┌────────────────────────────────────────┐
                                                  │ Kafka Topics: enriched.*, aggregated.* │
                                                  └────────────────────┬───────────────────┘
                                                                       ▼
                                                         ┌────────────────────────────┐
                                                         │  Python Kafka Consumer     │
                                                         └────────┬───────────┬───────┘
                                                                  ▼           ▼
                                                           ┌──────────┐  ┌────────────┐
                                                           │  Redis   │  │ PostgreSQL │
                                                           └────┬─────┘  └────────────┘
                                                                ▼
                                                         ┌────────────┐
                                                         │  FastAPI   │
                                                         └────────────┘


```

## 🧱 Project Structure

```
mobility-hub/
├── api/                      # Lightweight Redis API
├── ingestion/                # Trip event Kafka producer (simulator)
├── flink_jobs/               # Flink preprocessing and aggregation jobs
├── kafka_consumers/          # Kafka to Redis/Postgres sinks
├── data/                     # Input datasets (.parquet files)
├── Dockerfile.*              # Container specs for services
├── docker-compose.yml        # Container orchestration
├── create_topics.sh          # Kafka topic creation script
└── submit_all_jobs.sh        # One-click Flink job submitter
```

## 🔄 Kafka Consumer Sink

A standalone Python service (`kafka_consumers/`) listens to `aggregated.*` Kafka topics and writes final metrics to both:

- **Redis** — for real-time, low-latency API access
- **PostgreSQL** — for persistent storage

This component ensures decoupling between stream processing (Flink) and serving layers (API, BI tools).

## 🛠️ Technologies Used

- **Apache Kafka** – Streaming backbone
- **Apache Flink** – Stream processing and aggregation
- **Redis** – In-memory cache for low-latency API access
- **PostgreSQL** (optional) – Persisted storage
- **Python** – All Flink and API logic
- **Docker** – Full stack orchestration

## 🛡️ Fault Tolerance & Windowing

- Flink jobs use **5-minute tumbling windows** for aggregations.
- Checkpointing is enabled every 5 minutes to ensure recovery in case of failure.

## ⚙️ Setup & Run

```bash
# 1. Create and activate a virtual environment
python3 -m venv .venv
source .venv/bin/activate
pip install -r ingestion/requirements.txt

# 2. Start all services
docker compose up -d --build

  # 🧠 If you are on Apple Silicon (M*) and encounter architecture issues:
  # 👉 Run with platform override to enforce amd64-based containers
    # 👉 If you are using Docker Compose V2 (Recommended):
    # Use platform override directly with --platform flag
  docker compose --platform=linux/amd64 up -d --build
  
    # 👉 If you are using Docker Compose V1 (Legacy):
    # --platform flag is NOT supported, so use environment variable instead
    # Run the following command instead:
  DOCKER_DEFAULT_PLATFORM=linux/amd64 docker-compose up -d --build

# 3. Wait for Kafka and Flink to be ready (approx. 30 seconds)
sleep 30 # Ensures services are up before topic creation

# 4. Create Kafka topics
./create_topics.sh

# 👉 Run this in a separate terminal
# 5. Start streaming trip events into Kafka (runs indefinitely)
python3 ingestion/producer.py

# 6. Submit Flink jobs
./submit_all_jobs.sh
```

### create_topics.sh

A helper shell script that creates all required Kafka topics consistently, with control over key configuration options such as:

- **Partition count** (for parallelism and scalability)
- **Replication factor** (for fault-tolerance)
- **Topic naming conventions** (to separate enriched vs aggregated streams)


## 🗃️ Output Storage

- Aggregated metrics are written to:
  - **Redis** for fast, in-memory real-time access
  - **PostgreSQL** for long-term persistent storage (optional)
  - 
### 🧠 Metric Upserts & Reconciliation

The Kafka consumer performs:

- **Weighted average merging** for fields like `avg_duration`
- **Redis updates** with expiry (`EX 86400`) for in-memory freshness
- **PostgreSQL upserts** to ensure persistence without duplication, using conflict resolution logic based on primary keys.

## 🌐 Real-Time API (FastAPI)

The `api/redis_api.py` service exposes a lightweight, low-latency API using **FastAPI**. It interacts with **Redis** to serve:

- Entity-level real-time metrics (e.g., `/metrics/driver/<id>`)
- Key listings for any metric entity (e.g., `/metrics/passenger`)

Ideal for real-time dashboards or analytics integrations.

### 📊 API Access

Once the system is running:

- Access metrics via:
  ```
  GET http://localhost:8000/metrics/<entity_type>/<entity_id>
  ```

Example:
```bash
curl http://localhost:8000/metrics/driver/driver_123
```

## 🧪 Sample Data

Located in the `data/` directory:

- `trips.parquet`
- `drivers.parquet`
- `passengers.parquet`

## 🧩 Aggregation Types

Each Flink job listens to an `enriched.*` topic and produces windowed aggregates:

| Job                   | Kafka Input Topic       | Kafka Output Topic        |
|------------------------|--------------------------|----------------------------|
| `driver_job.py`        | `enriched.driver`        | `aggregated.driver`       |
| `passenger_job.py`     | `enriched.passenger`     | `aggregated.passenger`    |
| `vehicle_type_job.py`  | `enriched.vehicle_type`  | `aggregated.vehicle_type` |
| `h3_metrics_job.py`    | `enriched.h3`            | `aggregated.h3`           |

> Note: The `h3_region` is a placeholder for `start_location_id`, **not an actual H3 spatial index**.

## 🧬 Data Layering: From Raw to Aggregated

The system follows a multi-stage Kafka topic structure:

1. **Raw Layer** — `trips_stream`: Raw trip events simulated from `.parquet` files.
2. **Processed Layer** — `enriched.*`: Enriched records partitioned by entity type.
3. **Aggregated Layer** — `aggregated.*`: Windowed metrics ready for BI, ML, or API access.

This separation ensures better observability, debug-ability, and extensibility across data stages.

## 🧩 Data Handling Utilities

- Kafka messages are parsed with `parse_enriched_json()` for each entity.
- Aggregations are done using `AggregateMetrics`, a shared `ReduceFunction`.
- These helpers reside in `flink_jobs/helpers.py`.

## 🧠 Extensibility

- Add new enrichment fields via `preprocessing_job.py`
- Integrate real H3 geospatial indexing with `h3-py` or Uber’s H3 library
- Plug in different sinks (e.g., BigQuery, Delta Lake)
- Extend API with filters, analytics, or real-time dashboards

