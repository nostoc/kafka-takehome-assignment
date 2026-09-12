"""
Kafka Avro Order Consumer.
Consumes purchase orders, performs real-time running average price aggregation,
implements exponential backoff retry logic, and routes failed messages to Dead Letter Queue (DLQ).
"""
import datetime
import json
import time
from collections import defaultdict
from typing import Dict, Any, Optional

from confluent_kafka import Consumer, Producer, KafkaError, KafkaException
from confluent_kafka.serialization import SerializationContext, MessageField
from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table

from src.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    SCHEMA_REGISTRY_URL,
    TOPIC_ORDERS,
    TOPIC_RETRY,
    TOPIC_DLQ,
    CONSUMER_GROUP_ORDERS,
    MAX_RETRY_ATTEMPTS,
    INITIAL_BACKOFF_SECONDS,
    BACKOFF_MULTIPLIER,
)
from src.schema_utils import (
    get_schema_registry_client,
    get_order_deserializer,
    get_dlq_serializer,
)

console = Console()


class MetricsAggregator:
    """Maintains real-time running price aggregation across all received orders."""

    def __init__(self):
        self.total_orders: int = 0
        self.total_revenue: float = 0.0
        self.min_price: float = float("inf")
        self.max_price: float = 0.0
        self.product_counts = defaultdict(int)
        self.product_revenue = defaultdict(float)
        self.retried_count: int = 0
        self.dlq_count: int = 0

    @property
    def running_average(self) -> float:
        return self.total_revenue / self.total_orders if self.total_orders > 0 else 0.0

    def add_order(self, product: str, price: float):
        self.total_orders += 1
        self.total_revenue += price
        self.min_price = min(self.min_price, price)
        self.max_price = max(self.max_price, price)
        self.product_counts[product] += 1
        self.product_revenue[product] += price

    def get_summary_table(self) -> Table:
        table = Table(title="[bold green]Real-Time Order Price Aggregation[/bold green]", expand=True)
        table.add_column("Metric", style="cyan", no_wrap=True)
        table.add_column("Value", style="bold yellow")

        table.add_row("Total Orders Processed", str(self.total_orders))
        table.add_row("Total Revenue", f"${self.total_revenue:,.2f}")
        table.add_row("[bold]Running Average Price[/bold]", f"[bold green]${self.running_average:,.2f}[/bold green]")
        min_p_str = f"${self.min_price:,.2f}" if self.min_price != float("inf") else "$0.00"
        table.add_row("Min / Max Price", f"{min_p_str} / ${self.max_price:,.2f}")
        table.add_row("Retried Orders (Recovered/Attempted)", f"[yellow]{self.retried_count}[/yellow]")
        table.add_row("Dead Letter Queue (DLQ) Routed", f"[bold red]{self.dlq_count}[/bold red]")
        return table

    def get_product_table(self) -> Table:
        table = Table(title="[bold blue]Product-Level Price Aggregation[/bold blue]", expand=True)
        table.add_column("Product", style="cyan")
        table.add_column("Count", justify="right")
        table.add_column("Total Volume", justify="right", style="green")
        table.add_column("Avg Price", justify="right", style="bold yellow")

        for prod, count in sorted(self.product_counts.items(), key=lambda x: x[1], reverse=True)[:6]:
            tot = self.product_revenue[prod]
            avg = tot / count if count > 0 else 0.0
            table.add_row(prod, str(count), f"${tot:,.2f}", f"${avg:,.2f}")
        return table


class OrderConsumer:
    def __init__(
        self,
        bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
        schema_registry_url: str = SCHEMA_REGISTRY_URL,
        group_id: str = CONSUMER_GROUP_ORDERS,
    ):
        self.bootstrap_servers = bootstrap_servers
        self.schema_registry_url = schema_registry_url
        self.sr_client = get_schema_registry_client(schema_registry_url)
        self.avro_deserializer = get_order_deserializer(self.sr_client)
        self.dlq_serializer = get_dlq_serializer(self.sr_client)

        # Kafka Consumer Configuration
        self.consumer = Consumer({
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
        })

        # Producer for Retry and DLQ topics
        self.producer = Producer({"bootstrap.servers": bootstrap_servers})
        self.metrics = MetricsAggregator()

    def send_to_dlq(
        self,
        order_id: str,
        product: Optional[str],
        price: Optional[float],
        raw_payload: str,
        error_msg: str,
        retry_count: int,
    ):
        """Route unrecoverable or max-retried message to the Dead Letter Queue."""
        self.metrics.dlq_count += 1
        dlq_payload = {
            "orderId": order_id or "UNKNOWN",
            "product": product,
            "price": float(price) if price is not None else None,
            "originalPayload": str(raw_payload)[:500],
            "errorMessage": str(error_msg),
            "retryCount": int(retry_count),
            "failedAt": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        }

        try:
            serialized_dlq = self.dlq_serializer(
                dlq_payload,
                SerializationContext(TOPIC_DLQ, MessageField.VALUE),
            )
            self.producer.produce(
                topic=TOPIC_DLQ,
                key=order_id.encode("utf-8") if order_id else b"UNKNOWN",
                value=serialized_dlq,
            )
            self.producer.poll(0)
            console.print(
                f"[bold red][DLQ ROUTED][/bold red] Order [cyan]{order_id}[/cyan] -> "
                f"Topic [magenta]{TOPIC_DLQ}[/magenta] | Reason: [italic]{error_msg}[/italic]"
            )
        except Exception as e:
            console.print(f"[bold red][DLQ SERIALIZE ERROR][/bold red] Failed to publish to DLQ: {e}")

    def send_to_retry_topic(self, raw_value: bytes, key: Optional[bytes], current_retries: int, order_data: Dict[str, Any]):
        """Publish message to retry topic with incremented retry header."""
        self.metrics.retried_count += 1
        next_retry = current_retries + 1
        backoff = INITIAL_BACKOFF_SECONDS * (BACKOFF_MULTIPLIER ** (next_retry - 1))
        
        console.print(
            f"[bold yellow][RETRY ATTEMPT {next_retry}/{MAX_RETRY_ATTEMPTS}][/bold yellow] "
            f"Order [cyan]{order_data.get('orderId', 'N/A')}[/cyan] scheduled for retry with backoff {backoff:.1f}s"
        )
        time.sleep(backoff)

        headers = [("retry_count", str(next_retry).encode("utf-8"))]
        self.producer.produce(
            topic=TOPIC_RETRY,
            key=key,
            value=raw_value,
            headers=headers,
        )
        self.producer.poll(0)

    def process_order(self, order: Dict[str, Any], retry_count: int = 0):
        """
        Business logic for processing an order.
        Simulates temporary and permanent failure scenarios for demonstration.
        """
        order_id = order.get("orderId", "N/A")
        product = str(order.get("product", ""))
        price = order.get("price")

        # Scenario 1: Permanent fatal failure simulation
        if product.startswith("FAIL_DLQ"):
            raise ValueError(f"Permanent validation error: Product '{product}' is blacklisted.")

        # Scenario 2: Temporary failure simulation
        if product.startswith("FAIL_RETRY"):
            if retry_count < 2:
                # Fails for first 2 attempts, then succeeds on 3rd attempt
                raise ConnectionError(f"Transient payment gateway timeout for product '{product}' (attempt {retry_count + 1})")
            else:
                console.print(f"[bold green][RETRY RECOVERED][/bold green] Order [cyan]{order_id}[/cyan] recovered successfully on retry #{retry_count}!")

        # Scenario 3: Invalid price validation
        if price is None or price <= 0:
            raise ValueError(f"Invalid price value: {price}")

        # Normal processing: Real-time aggregation
        self.metrics.add_order(product=product, price=float(price))
        console.print(
            f"[bold green][SUCCESS][/bold green] Processed Order [cyan]{order_id}[/cyan] | "
            f"Product: [italic]{product}[/italic] | Price: [green]${price:,.2f}[/green] | "
            f"Running Avg: [bold yellow]${self.metrics.running_average:,.2f}[/bold yellow] (N={self.metrics.total_orders})"
        )

    def run(self):
        """Main consumption loop."""
        topics = [TOPIC_ORDERS, TOPIC_RETRY]
        self.consumer.subscribe(topics)

        console.print(
            Panel.fit(
                "[bold green]Kafka Avro Order Consumer & Aggregator[/bold green]\n"
                f"Broker: [yellow]{self.bootstrap_servers}[/yellow] | Group: [cyan]{CONSUMER_GROUP_ORDERS}[/cyan]\n"
                f"Subscribed Topics: [blue]{topics}[/blue] | DLQ Topic: [magenta]{TOPIC_DLQ}[/magenta]\n"
                f"Max Retries: [yellow]{MAX_RETRY_ATTEMPTS}[/yellow]",
                title="Consumer Initialized",
                border_style="green",
            )
        )

        try:
            while True:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        console.print(f"[bold red]Kafka Consumer Error:[/bold red] {msg.error()}")
                        continue

                # Extract retry_count header if present
                retry_count = 0
                if msg.headers():
                    for k, v in msg.headers():
                        if k == "retry_count" and v:
                            try:
                                retry_count = int(v.decode("utf-8"))
                            except ValueError:
                                retry_count = 0

                raw_bytes = msg.value()
                key_bytes = msg.key()
                key_str = key_bytes.decode("utf-8", errors="ignore") if key_bytes else "UNKNOWN"

                # Step 1: Deserialization
                order = None
                try:
                    order = self.avro_deserializer(
                        raw_bytes,
                        SerializationContext(msg.topic(), MessageField.VALUE),
                    )
                except Exception as de_err:
                    # Deserialization failed -> Poison pill / corrupt message -> Send directly to DLQ
                    console.print(f"[bold red][DESERIALIZATION ERROR][/bold red] {de_err}")
                    self.send_to_dlq(
                        order_id=key_str,
                        product=None,
                        price=None,
                        raw_payload=repr(raw_bytes),
                        error_msg=f"Avro Deserialization Error: {de_err}",
                        retry_count=retry_count,
                    )
                    continue

                # Step 2: Processing & Business Logic with Retry & DLQ
                try:
                    self.process_order(order, retry_count=retry_count)
                except ConnectionError as transient_err:
                    # Temporary failure -> check retry limit
                    if retry_count < MAX_RETRY_ATTEMPTS:
                        self.send_to_retry_topic(raw_bytes, key_bytes, retry_count, order)
                    else:
                        # Max retries exceeded -> DLQ
                        self.send_to_dlq(
                            order_id=order.get("orderId", key_str),
                            product=order.get("product"),
                            price=order.get("price"),
                            raw_payload=json.dumps(order),
                            error_msg=f"Exceeded max retries ({MAX_RETRY_ATTEMPTS}). Last error: {transient_err}",
                            retry_count=retry_count,
                        )
                except Exception as perm_err:
                    # Permanent/fatal error -> Send directly to DLQ
                    self.send_to_dlq(
                        order_id=order.get("orderId", key_str),
                        product=order.get("product"),
                        price=order.get("price"),
                        raw_payload=json.dumps(order),
                        error_msg=f"Permanent failure: {perm_err}",
                        retry_count=retry_count,
                    )

        except KeyboardInterrupt:
            console.print("\n[yellow]Shutting down consumer...[/yellow]")
        finally:
            self.consumer.close()
            self.producer.flush()
            console.print("\n" + "=" * 60)
            console.print(self.metrics.get_summary_table())
            console.print(self.metrics.get_product_table())
            console.print("=" * 60)


if __name__ == "__main__":
    consumer = OrderConsumer()
    consumer.run()
