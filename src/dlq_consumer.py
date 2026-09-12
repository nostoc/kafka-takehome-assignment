"""
Dead Letter Queue (DLQ) Inspector & Consumer.
Monitors the `orders-dlq` topic and renders real-time diagnostics on permanently failed messages.
"""
import sys
from confluent_kafka import Consumer, KafkaError
from confluent_kafka.serialization import SerializationContext, MessageField
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from src.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    SCHEMA_REGISTRY_URL,
    TOPIC_DLQ,
    CONSUMER_GROUP_DLQ,
)
from src.schema_utils import (
    get_schema_registry_client,
    get_dlq_deserializer,
)

console = Console()


def run_dlq_monitor():
    sr_client = get_schema_registry_client(SCHEMA_REGISTRY_URL)
    dlq_deserializer = get_dlq_deserializer(sr_client)

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": CONSUMER_GROUP_DLQ,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })

    consumer.subscribe([TOPIC_DLQ])

    console.print(
        Panel.fit(
            "[bold red]Dead Letter Queue (DLQ) Real-Time Monitor[/bold red]\n"
            f"Broker: [yellow]{KAFKA_BOOTSTRAP_SERVERS}[/yellow] | "
            f"Topic: [magenta]{TOPIC_DLQ}[/magenta] | "
            f"Group: [cyan]{CONSUMER_GROUP_DLQ}[/cyan]",
            title="DLQ Monitor Initialized",
            border_style="red",
        )
    )

    dlq_count = 0
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                console.print(f"[bold red]DLQ Consumer Error:[/bold red] {msg.error()}")
                continue

            dlq_count += 1
            raw_bytes = msg.value()
            key_str = msg.key().decode("utf-8", errors="ignore") if msg.key() else "N/A"

            try:
                record = dlq_deserializer(raw_bytes, SerializationContext(TOPIC_DLQ, MessageField.VALUE))
                order_id = record.get("orderId", key_str)
                product = record.get("product") or "N/A"
                price = f"${record['price']:,.2f}" if record.get("price") is not None else "N/A"
                err_msg = record.get("errorMessage", "Unknown Error")
                retries = record.get("retryCount", 0)
                failed_at = record.get("failedAt", "N/A")
                payload = record.get("originalPayload", "")
            except Exception as e:
                order_id = key_str
                product = "N/A"
                price = "N/A"
                err_msg = f"Non-Avro DLQ Record: {raw_bytes[:80]} ({e})"
                retries = 0
                failed_at = "N/A"
                payload = repr(raw_bytes)[:100]

            table = Table(title=f"[bold red]DLQ Incident #{dlq_count}[/bold red]", border_style="red")
            table.add_column("Field", style="cyan", no_wrap=True)
            table.add_column("Details", style="bold white")

            table.add_row("Order ID", f"[yellow]{order_id}[/yellow]")
            table.add_row("Product", product)
            table.add_row("Price", price)
            table.add_row("Retry Count", str(retries))
            table.add_row("Failure Reason", f"[bold red]{err_msg}[/bold red]")
            table.add_row("Failed At", failed_at)
            table.add_row("Topic Partition & Offset", f"Partition {msg.partition()} @ Offset {msg.offset()}")
            table.add_row("Payload Snippet", f"[dim]{payload}[/dim]")

            console.print(table)

    except KeyboardInterrupt:
        console.print("\n[yellow]Stopping DLQ Monitor...[/yellow]")
    finally:
        consumer.close()
        console.print(f"[bold red]DLQ Session Ended. Total Dead-Lettered Messages Inspected: {dlq_count}[/bold red]")


if __name__ == "__main__":
    run_dlq_monitor()
