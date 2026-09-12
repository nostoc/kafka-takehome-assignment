# Kafka-Based Order Processing System with Avro Serialization

> **Big Data and Analytics (EC8202) — Take-Home Assignment**  
> A distributed event-driven order processing system built with Apache Kafka, Confluent Schema Registry, Avro serialization, real-time price aggregation, exponential backoff retry logic, and Dead Letter Queue (DLQ) fault handling.

---

## Architecture Overview

```mermaid
flowchart TD
    subgraph Producer ["Order Producer (src/producer.py)"]
        P[Randomized Purchase Generator] -->|Avro Serialized| KT[Topic: orders]
        P -.->|Fault / Poison Pill Injection| KT
    end

    subgraph KafkaCluster ["Kafka Cluster & Schema Registry"]
        SR[Confluent Schema Registry\n(schemas/order.avsc)]
        KT[Topic: orders]
        RT[Topic: orders-retry]
        DLQT[Topic: orders-dlq]
    end

    subgraph ConsumerEngine ["Order Consumer & Aggregator (src/consumer.py)"]
        KT --> C[Avro Deserializer]
        RT --> C
        C --> AGG[Real-Time Aggregator\nRunning Average Price]
        C -->|Transient Failure| RETRY[Retry Handler\nExponential Backoff]
        RETRY -->|Retry Count < Max| RT
        RETRY -->|Max Retries Exceeded| DLQT
        C -->|Permanent Error / Poison Pill| DLQT
    end

    subgraph Observability ["Observability & Monitoring"]
        DLQT --> DLQC[DLQ Monitor Inspector\nsrc/dlq_consumer.py]
        KafkaCluster --> KUI[Kafka UI Web Dashboard\nhttp://localhost:8080]
    end
```

---

## Key Features

1. **Avro Serialization**:
   - Strictly conforms to the specified `order.avsc` schema (`orderId`, `product`, `price`).
   - Integrated with Confluent Schema Registry for runtime schema enforcement.
2. **Real-Time Aggregation**:
   - Computes dynamic running average of order prices continuously on message arrival.
   - Computes total revenue, min/max price, order count, and product-level breakdown.
3. **Retry Logic for Temporary Failures**:
   - Handles transient failures with configurable retry attempts (`MAX_RETRY_ATTEMPTS = 3`).
   - Applies exponential backoff delay (`INITIAL_BACKOFF = 1.0s`, `MULTIPLIER = 2.0x`).
   - Propagates attempt counters across retries using Kafka message headers.
4. **Dead Letter Queue (DLQ)**:
   - Permanently failed orders (poison pills, unparseable payloads, or exhausted retries) are dispatched to `orders-dlq`.
   - Stores failure diagnostics, retry count, ISO timestamp, and original payload.
5. **Interactive Web Dashboard & Live Monitor**:
   - Full visibility into Kafka topics, schemas, and consumer offsets via Kafka UI (`http://localhost:8080`).
   - Rich terminal dashboard with formatted status panels and live incident tables.

---

## Order Avro Schema Definition (`schemas/order.avsc`)

```json
{
  "type": "record",
  "name": "Order",
  "namespace": "com.assignment.orders",
  "fields": [
    {"name": "orderId", "type": "string", "doc": "Unique identifier for the order"},
    {"name": "product", "type": "string", "doc": "Name of the purchased item"},
    {"name": "price", "type": "float", "doc": "Price of the product"}
  ]
}
```

---

## Project Structure

```text
take-home-assignment/
├── docker-compose.yml        # Docker services (Kafka KRaft, Schema Registry, Kafka UI)
├── requirements.txt          # Python dependencies
├── schemas/
│   ├── order.avsc            # Order message Avro schema definition
│   └── order_dlq.avsc        # Dead Letter Queue Avro schema definition
├── src/
│   ├── config.py             # System configuration, paths, and topic names
│   ├── schema_utils.py       # Avro schema loader, registry client & serializers
│   ├── producer.py           # Avro order producer with simulated fault injection
│   ├── consumer.py           # Order consumer with real-time aggregation, retry & DLQ
│   └── dlq_consumer.py       # Dedicated DLQ inspector for monitoring failed orders
├── scripts/
│   ├── setup_topics.py       # Kafka topic provisioning script
│   └── run_demo.py           # One-click demonstration helper
└── README.md                 # Documentation and video demonstration guide
```

---

## Quickstart & Setup Guide

### 1. Start Kafka, Schema Registry & Kafka UI
Ensure Docker Desktop is running, then start the services:
```bash
docker compose up -d
```
Verify that all containers are healthy:
- Kafka Broker: `localhost:9092`
- Schema Registry: `http://localhost:8081`
- Kafka UI: `http://localhost:8080`

### 2. Set Up Python Environment
```bash
python -m venv .venv
.venv\Scripts\activate      # Windows (or `source .venv/bin/activate` on Linux/macOS)
pip install -r requirements.txt
```

### 3. Initialize Kafka Topics
```bash
python scripts/setup_topics.py
```
This provisions `orders`, `orders-retry`, and `orders-dlq`.

---

## Demonstration Guide (< 5 Minute Video)

Follow this 4-step walkthrough during your recording:

### Terminal Layout (3 Split Terminals)

| Terminal 1 (Consumer) | Terminal 2 (DLQ Monitor) |
|---|---|
| `python src/consumer.py` | `python src/dlq_consumer.py` |

| Terminal 3 (Producer) | Browser Window |
|---|---|
| `python src/producer.py --count 20 --interval 1.0 --fault-rate 0.3` | `http://localhost:8080` (Kafka UI) |

---

### Step-by-Step Video Script (Under 5 Minutes)

1. **Introduction (~30 seconds)**:
   - Introduce project title: *Kafka Order Processing System with Avro Serialization, Real-Time Aggregation, Retry Logic & DLQ*.
   - Briefly show `docker-compose.yml` and `schemas/order.avsc`.

2. **Start Consumers (~30 seconds)**:
   - In **Terminal 1**, run `python src/consumer.py` (shows live aggregator waiting for orders).
   - In **Terminal 2**, run `python src/dlq_consumer.py` (shows DLQ monitor waiting for failed messages).

3. **Produce Orders with Fault Injection (~2 minutes)**:
   - In **Terminal 3**, run:
     ```bash
     python src/producer.py --count 15 --interval 1.0 --fault-rate 0.3
     ```
   - **Explain Live Output**:
     - **Normal Orders** (`[NORMAL]`): Consumed, updating total orders, total revenue, and running average price in real time.
     - **Transient Faults** (`[TRIGGER-RETRY]`): Triggers exponential backoff retry attempts (attempts 1 & 2 fail, attempt 3 succeeds and recovers).
     - **Permanent Faults** (`[TRIGGER-DLQ]`): Validation failure routed straight to DLQ.
     - Observe **Terminal 2** instantly displaying the structured DLQ incident table with diagnosis.

4. **Poison Pill Demonstration (~1 minute)**:
   - In **Terminal 3**, run:
     ```bash
     python src/producer.py --inject-corrupt
     ```
   - Shows malformed non-Avro bytes handled gracefully by consumer and routed to DLQ without crashing the pipeline.

5. **Kafka UI & Conclusion (~45 seconds)**:
   - Switch to browser at `http://localhost:8080`.
   - Show topics: `orders`, `orders-retry`, `orders-dlq`.
   - Show registered Avro schema in Schema Registry.
   - Stop consumers to display final summary metrics and product-level aggregation table.

---

## Configuration Reference

| Parameter | Environment Variable | Default Value | Description |
|---|---|---|---|
| Kafka Bootstrap | `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker address |
| Schema Registry | `SCHEMA_REGISTRY_URL` | `http://localhost:8081` | Confluent Schema Registry endpoint |
| Order Topic | `TOPIC_ORDERS` | `orders` | Main order stream topic |
| Retry Topic | `TOPIC_RETRY` | `orders-retry` | Topic for exponential backoff retries |
| DLQ Topic | `TOPIC_DLQ` | `orders-dlq` | Dead Letter Queue topic |
| Max Retries | `MAX_RETRY_ATTEMPTS` | `3` | Maximum retry attempts before DLQ |
| Initial Backoff | `INITIAL_BACKOFF_SECONDS` | `1.0` | Base backoff duration (seconds) |
| Backoff Multiplier | `BACKOFF_MULTIPLIER` | `2.0` | Exponential factor for backoff |
