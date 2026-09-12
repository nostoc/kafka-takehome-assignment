"""
Kafka Avro Order Producer.
Generates order messages, serializes them with Avro, and produces to Kafka.
Supports configurable fault injection to demonstrate retry logic and DLQ.
"""
import argparse
import random
import time
import uuid
from typing import Dict, Any, Optional

from confluent_kafka import Producer
from confluent_kafka.serialization import SerializationContext, MessageField
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

from src.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    SCHEMA_REGISTRY_URL,
    TOPIC_ORDERS,
)
from src.schema_utils import (
    get_schema_registry_client,
    get_order_serializer,
)

console = Console()

PRODUCTS = [
    "Laptop Pro 16",
    "Wireless Noise-Canceling Headphones",
    "Ultra-Wide 4K Gaming Monitor",
    "Mechanical RGB Keyboard",
    "Ergonomic Optical Mouse",
    "Smartphone 5G",
    "Tablet Air 11-inch",
    "Smart Fitness Watch",
    "USB-C Multiport Dock",
    "External 2TB NVMe SSD",
]

PRODUCT_PRICE_RANGES = {
    "Laptop Pro 16": (999.99, 2499.99),
    "Wireless Noise-Canceling Headphones": (149.99, 399.99),
    "Ultra-Wide 4K Gaming Monitor": (349.99, 899.99),
    "Mechanical RGB Keyboard": (79.99, 199.99),
    "Ergonomic Optical Mouse": (39.99, 119.99),
    "Smartphone 5G": (599.99, 1299.99),
    "Tablet Air 11-inch": (449.99, 899.99),
    "Smart Fitness Watch": (129.99, 349.99),
    "USB-C Multiport Dock": (49.99, 129.99),
    "External 2TB NVMe SSD": (109.99, 229.99),
}


class OrderProducer:
    def __init__(self, bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS, schema_registry_url: str = SCHEMA_REGISTRY_URL):
        self.bootstrap_servers = bootstrap_servers
        self.schema_registry_url = schema_registry_url
        self.producer = Producer({"bootstrap.servers": bootstrap_servers})
        self.sr_client = get_schema_registry_client(schema_registry_url)
        self.avro_serializer = get_order_serializer(self.sr_client)
        self._order_counter = 1000

    def generate_order(self, fault_type: Optional[str] = None) -> Dict[str, Any]:
        """Generate an order dictionary matching order.avsc."""
        self._order_counter += 1
        order_id = f"ORD-{self._order_counter}"

        if fault_type == "temporary_failure":
            # Product tagged to trigger simulated temporary failure & retry in consumer
            product = f"FAIL_RETRY_Item_{random.randint(1, 99)}"
            price = round(random.uniform(50.0, 300.0), 2)
        elif fault_type == "permanent_failure":
            # Product tagged to trigger fatal unrecoverable processing error -> DLQ
            product = f"FAIL_DLQ_Item_{random.randint(1, 99)}"
            price = round(random.uniform(10.0, 50.0), 2)
        else:
            product = random.choice(PRODUCTS)
            min_p, max_p = PRODUCT_PRICE_RANGES[product]
            price = round(random.uniform(min_p, max_p), 2)

        return {
            "orderId": order_id,
            "product": product,
            "price": float(price),
        }

    def delivery_report(self, err, msg, order_data: Dict[str, Any]):
        """Callback triggered upon Kafka message delivery or failure."""
        if err is not None:
            console.print(f"[bold red][ERROR][/bold red] Message delivery failed: {err}")
        else:
            order_id = order_data.get("orderId", "N/A")
            product = order_data.get("product", "N/A")
            price = order_data.get("price", 0.0)
            
            style = "bold green"
            tag = "[NORMAL]"
            if str(product).startswith("FAIL_RETRY"):
                style = "bold yellow"
                tag = "[TRIGGER-RETRY]"
            elif str(product).startswith("FAIL_DLQ"):
                style = "bold magenta"
                tag = "[TRIGGER-DLQ]"

            console.print(
                f"[{style}]{tag}[/{style}] Produced Order [cyan]{order_id}[/cyan] | "
                f"Product: [italic]{product}[/italic] | "
                f"Price: [green]${price:,.2f}[/green] -> "
                f"Topic: [blue]{msg.topic()}[/blue] [Partition {msg.partition()} @ Offset {msg.offset()}]"
            )

    def produce_order(self, order: Dict[str, Any], topic: str = TOPIC_ORDERS):
        """Serialize and produce a single order."""
        try:
            serialized_payload = self.avro_serializer(
                order,
                SerializationContext(topic, MessageField.VALUE),
            )
            self.producer.produce(
                topic=topic,
                key=str(order["orderId"]).encode("utf-8"),
                value=serialized_payload,
                on_delivery=lambda err, msg: self.delivery_report(err, msg, order),
            )
            self.producer.poll(0)
        except Exception as e:
            console.print(f"[bold red]Serialization/Publish Exception:[/bold red] {e}")

    def produce_corrupted_message(self, topic: str = TOPIC_ORDERS):
        """Produce raw non-Avro corrupt bytes to demonstrate consumer DLQ poison-pill handling."""
        self._order_counter += 1
        fake_id = f"CORRUPT-{self._order_counter}"
        garbage_bytes = b"\x00\x01\x02\x03CORRUPTED_NON_AVRO_BINARY_DATA_GARBAGE"
        console.print(f"[bold red][POISON-PILL][/bold red] Sending malformed raw bytes to topic [blue]{topic}[/blue]...")
        self.producer.produce(
            topic=topic,
            key=fake_id.encode("utf-8"),
            value=garbage_bytes,
            on_delivery=lambda err, msg: console.print(
                f"[bold red][POISON-PILL DELIVERED][/bold red] Corrupt msg at Partition {msg.partition()} @ Offset {msg.offset()}"
            ) if not err else console.print(f"[red]Error: {err}[/red]",
        ))
        self.producer.poll(0)

    def flush(self, timeout: float = 5.0):
        self.producer.flush(timeout)


def main():
    parser = argparse.ArgumentParser(description="Kafka Avro Order Producer")
    parser.add_argument("--count", type=int, default=0, help="Number of orders to produce (0 for continuous)")
    parser.add_argument("--interval", type=float, default=1.5, help="Interval between orders in seconds")
    parser.add_argument("--fault-rate", type=float, default=0.25, help="Probability of injecting a fault (0.0 to 1.0)")
    parser.add_argument("--inject-corrupt", action="store_true", help="Send a malformed raw non-Avro byte payload")
    args = parser.parse_args()

    console.print(
        Panel.fit(
            "[bold cyan]Kafka Avro Order Producer[/bold cyan]\n"
            f"Broker: [yellow]{KAFKA_BOOTSTRAP_SERVERS}[/yellow] | "
            f"Schema Registry: [yellow]{SCHEMA_REGISTRY_URL}[/yellow]\n"
            f"Topic: [blue]{TOPIC_ORDERS}[/blue] | Fault Rate: [magenta]{args.fault_rate * 100:.0f}%[/magenta]",
            title="Producer Initialized",
            border_style="cyan",
        )
    )

    producer = OrderProducer()

    if args.inject_corrupt:
        producer.produce_corrupted_message()
        producer.flush()
        return

    produced_count = 0
    try:
        while True:
            # Determine fault type based on fault rate
            fault_type = None
            if args.fault_rate > 0 and random.random() < args.fault_rate:
                fault_type = random.choice(["temporary_failure", "temporary_failure", "permanent_failure"])

            order = producer.generate_order(fault_type=fault_type)
            producer.produce_order(order)
            produced_count += 1

            if args.count > 0 and produced_count >= args.count:
                break

            time.sleep(args.interval)

    except KeyboardInterrupt:
        console.print("\n[yellow]Stopping producer gracefully...[/yellow]")
    finally:
        console.print("[dim]Flushing outstanding Kafka messages...[/dim]")
        producer.flush()
        console.print(f"[bold green]Successfully produced {produced_count} orders.[/bold green]")


if __name__ == "__main__":
    main()
