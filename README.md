# Kafka Order Processing System

A distributed order processing system built with Apache Kafka, Confluent Schema Registry, Avro serialization, real-time price aggregation, retry logic with exponential backoff, and a Dead Letter Queue (DLQ).

---

## Prerequisites

- **Docker** & **Docker Compose**
- **Python 3.8+**

---

## Getting Started

### 1. Start Kafka Services

Start Kafka, Schema Registry, and Kafka UI using Docker:

```bash
docker compose up -d
```

Service endpoints:
- **Kafka Broker**: `localhost:9092`
- **Schema Registry**: `http://localhost:8081`
- **Kafka UI**: `http://localhost:8080`

### 2. Set Up Python Environment

```bash
# Create and activate virtual environment
python -m venv .venv
.venv\Scripts\activate      # Windows (or `source .venv/bin/activate` on Linux/macOS)

# Install dependencies
pip install -r requirements.txt
```

### 3. Create Kafka Topics

Initialize the required topics (`orders`, `orders-retry`, `orders-dlq`):

```bash
python scripts/setup_topics.py
```

---

## Running the Application

Open separate terminal windows and run the following in order:

### Terminal 1: Consumer & Real-time Aggregator
Consumes orders, computes running metrics, and handles retries / DLQ routing:
```bash
python src/consumer.py
```

### Terminal 2: DLQ Monitor (Optional)
Monitors permanently failed messages and poison pills routed to the DLQ:
```bash
python src/dlq_consumer.py
```

### Terminal 3: Order Producer
Generate and send Avro-serialized orders:

```bash
# Generate 15 orders with simulated transient/permanent faults
python src/producer.py --count 15 --interval 1.0 --fault-rate 0.3

# (Optional) Inject corrupted/poison-pill messages to test DLQ
python src/producer.py --inject-corrupt
```

---

## Monitoring

Open [http://localhost:8080](http://localhost:8080) in your browser to view topics, messages, consumer groups, and schemas via Kafka UI.

---

## Teardown

To stop the Docker services:

```bash
docker compose down
```
