import sys
import time
from pathlib import Path

# Add project root to python path
ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from confluent_kafka.admin import AdminClient, NewTopic
from src.config import KAFKA_BOOTSTRAP_SERVERS, TOPIC_ORDERS, TOPIC_RETRY, TOPIC_DLQ


def wait_for_kafka(bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS, timeout_sec: int = 60) -> AdminClient:
    """Poll Kafka broker until it responds to metadata requests."""
    print(f"Connecting to Kafka at {bootstrap_servers}...")
    start_time = time.time()
    admin_client = AdminClient({"bootstrap.servers": bootstrap_servers})

    while time.time() - start_time < timeout_sec:
        try:
            metadata = admin_client.list_topics(timeout=5)
            if metadata.brokers:
                print(f"[OK] Connected to Kafka! Active Brokers: {len(metadata.brokers)}")
                return admin_client
        except Exception as e:
            print(f"Waiting for Kafka to be ready... ({e})")
        time.sleep(3)

    raise TimeoutError(f"Could not connect to Kafka at {bootstrap_servers} within {timeout_sec}s")


def create_topics(admin_client: AdminClient, num_partitions: int = 1, replication_factor: int = 1):
    """Ensure required topics exist."""
    required_topics = [TOPIC_ORDERS, TOPIC_RETRY, TOPIC_DLQ]
    metadata = admin_client.list_topics(timeout=10)
    existing_topics = set(metadata.topics.keys())

    new_topics = []
    for topic_name in required_topics:
        if topic_name not in existing_topics:
            print(f"Queueing creation for topic: {topic_name}")
            new_topics.append(NewTopic(topic_name, num_partitions=num_partitions, replication_factor=replication_factor))
        else:
            print(f"[INFO] Topic '{topic_name}' already exists.")

    if new_topics:
        fs = admin_client.create_topics(new_topics)
        for topic, f in fs.items():
            try:
                f.result()
                print(f"[SUCCESS] Created topic: {topic}")
            except Exception as e:
                print(f"[ERROR] Failed to create topic {topic}: {e}")


if __name__ == "__main__":
    client = wait_for_kafka()
    create_topics(client)
    print("Kafka topics setup completed successfully.")
