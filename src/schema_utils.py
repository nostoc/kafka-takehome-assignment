"""
Avro schema loading, registration, and serialization utilities.
"""
import io
import json
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import fastavro
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

from src.config import SCHEMA_REGISTRY_URL, ORDER_SCHEMA_PATH, DLQ_SCHEMA_PATH


def load_schema_str(schema_path: Path) -> str:
    """Load Avro schema JSON from file as string."""
    with open(schema_path, "r", encoding="utf-8") as f:
        return f.read()


def load_parsed_schema(schema_path: Path) -> Dict[str, Any]:
    """Load and parse Avro schema JSON from file."""
    with open(schema_path, "r", encoding="utf-8") as f:
        return json.load(f)


def get_schema_registry_client(url: str = SCHEMA_REGISTRY_URL) -> SchemaRegistryClient:
    """Initialize Confluent Schema Registry Client."""
    return SchemaRegistryClient({"url": url})


def get_order_serializer(sr_client: SchemaRegistryClient) -> AvroSerializer:
    """Create AvroSerializer for Order schema with Confluent Schema Registry."""
    schema_str = load_schema_str(ORDER_SCHEMA_PATH)
    return AvroSerializer(
        schema_registry_client=sr_client,
        schema_str=schema_str,
        to_dict=lambda obj, ctx: obj if isinstance(obj, dict) else obj.__dict__,
    )


def get_order_deserializer(sr_client: SchemaRegistryClient) -> AvroDeserializer:
    """Create AvroDeserializer for Order schema with Confluent Schema Registry."""
    schema_str = load_schema_str(ORDER_SCHEMA_PATH)
    return AvroDeserializer(
        schema_registry_client=sr_client,
        schema_str=schema_str,
        from_dict=lambda obj, ctx: obj,
    )


def get_dlq_serializer(sr_client: SchemaRegistryClient) -> AvroSerializer:
    """Create AvroSerializer for DLQ schema with Confluent Schema Registry."""
    schema_str = load_schema_str(DLQ_SCHEMA_PATH)
    return AvroSerializer(
        schema_registry_client=sr_client,
        schema_str=schema_str,
        to_dict=lambda obj, ctx: obj if isinstance(obj, dict) else obj.__dict__,
    )


def get_dlq_deserializer(sr_client: SchemaRegistryClient) -> AvroDeserializer:
    """Create AvroDeserializer for DLQ schema with Confluent Schema Registry."""
    schema_str = load_schema_str(DLQ_SCHEMA_PATH)
    return AvroDeserializer(
        schema_registry_client=sr_client,
        schema_str=schema_str,
        from_dict=lambda obj, ctx: obj,
    )


# FastAvro helpers for standalone encoding / decoding & validation
def fastavro_encode(record: Dict[str, Any], schema_path: Path = ORDER_SCHEMA_PATH) -> bytes:
    """Serialize record using fastavro without schema registry."""
    schema = load_parsed_schema(schema_path)
    parsed_schema = fastavro.parse_schema(schema)
    bytes_writer = io.BytesIO()
    fastavro.schemaless_writer(bytes_writer, parsed_schema, record)
    return bytes_writer.getvalue()


def fastavro_decode(raw_bytes: bytes, schema_path: Path = ORDER_SCHEMA_PATH) -> Dict[str, Any]:
    """Deserialize record using fastavro without schema registry."""
    schema = load_parsed_schema(schema_path)
    parsed_schema = fastavro.parse_schema(schema)
    bytes_reader = io.BytesIO(raw_bytes)
    return fastavro.schemaless_reader(bytes_reader, parsed_schema)


def validate_order(record: Dict[str, Any], schema_path: Path = ORDER_SCHEMA_PATH) -> Tuple[bool, Optional[str]]:
    """Validate record against Avro schema definition."""
    schema = load_parsed_schema(schema_path)
    parsed_schema = fastavro.parse_schema(schema)
    try:
        fastavro.validate(record, parsed_schema)
        return True, None
    except Exception as e:
        return False, str(e)
