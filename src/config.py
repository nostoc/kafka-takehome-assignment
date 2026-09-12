"""
Configuration settings for the Kafka Order Processing System.
"""
import os
from pathlib import Path

# Paths
BASE_DIR = Path(__file__).resolve().parent.parent
SCHEMAS_DIR = BASE_DIR / "schemas"
ORDER_SCHEMA_PATH = SCHEMAS_DIR / "order.avsc"
DLQ_SCHEMA_PATH = SCHEMAS_DIR / "order_dlq.avsc"

# Kafka Connection (use 127.0.0.1 for explicit IPv4 routing)
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://127.0.0.1:8081")

# Topics
TOPIC_ORDERS = os.getenv("TOPIC_ORDERS", "orders")
TOPIC_RETRY = os.getenv("TOPIC_RETRY", "orders-retry")
TOPIC_DLQ = os.getenv("TOPIC_DLQ", "orders-dlq")

# Consumer Groups
CONSUMER_GROUP_ORDERS = os.getenv("CONSUMER_GROUP_ORDERS", "order-processing-group")
CONSUMER_GROUP_DLQ = os.getenv("CONSUMER_GROUP_DLQ", "order-dlq-monitor-group")

# Resilience & Retry Configuration
MAX_RETRY_ATTEMPTS = int(os.getenv("MAX_RETRY_ATTEMPTS", "3"))
INITIAL_BACKOFF_SECONDS = float(os.getenv("INITIAL_BACKOFF_SECONDS", "1.0"))
BACKOFF_MULTIPLIER = float(os.getenv("BACKOFF_MULTIPLIER", "2.0"))
