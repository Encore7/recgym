"""
Avro serializer factory for RetailEvent values.

Responsible for:
- Loading the RetailEvent Avro schema from disk
- Creating an AvroSerializer bound to the Schema Registry
"""

import logging
import os
from typing import Any, Dict

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

from apps.generator.src.core.config import SchemaRegistrySettings
from apps.generator.src.infra.schema_registry import SUBJECT_TO_FILE
from libs.models.events import RetailEvent


logger = logging.getLogger(__name__)


def build_retail_event_serializer(
    cfg: SchemaRegistrySettings,
) -> AvroSerializer:
    """
    Build an AvroSerializer for the RetailEvent value subject.

    Args:
        cfg: Schema Registry settings.

    Returns:
        Configured AvroSerializer instance.

    Raises:
        SystemExit: If schema file cannot be found or read.
    """
    client = SchemaRegistryClient({"url": cfg.url})
    filename = SUBJECT_TO_FILE["raw-events-value"]
    schema_path = os.path.join(cfg.schema_dir, filename)

    try:
        with open(schema_path, encoding="utf-8") as f:
            schema_str = f.read()
    except FileNotFoundError as exc:
        logger.exception(
            "RetailEvent Avro schema file not found.",
            extra={"schema_path": schema_path},
        )
        raise SystemExit(1) from exc
    except Exception as exc:  # noqa: BLE001
        logger.exception(
            "Unexpected error while reading RetailEvent Avro schema.",
            extra={"schema_path": schema_path},
        )
        raise SystemExit(1) from exc

    def to_dict(event: RetailEvent, ctx: Dict[str, Any]) -> Dict[str, Any]:  # type: ignore[override]
        """
        Serializer hook converting RetailEvent → dict for AvroSerializer.

        Args:
            event: RetailEvent instance.
            ctx: Serialization context (unused).
        """
        return event.model_dump()

    serializer = AvroSerializer(
        schema_registry_client=client,
        schema_str=schema_str,
        to_dict=to_dict,  # type: ignore[arg-type]
    )
    return serializer
