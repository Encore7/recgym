"""
Schema Registry registration for Avro schemas.

This module is intentionally minimal for the generator service.

We only register the RetailEvent Avro schema because:
- Generator produces raw events to Kafka (events_raw)
- Downstream Flink + Feast no longer use Kafka for user/item features
- Realtime features go directly to Redis (Feast online store)
- Therefore, user/item feature schemas are not needed in Schema Registry
"""

import logging
import os
from typing import Mapping

from confluent_kafka.schema_registry import Schema, SchemaRegistryClient

from apps.generator.src.core.config import SchemaRegistrySettings

logger = logging.getLogger(__name__)

# Subject → Schema mapping used by Schema Registry.
SUBJECT_TO_FILE = {
    "raw-events-value": "RetailEvent.avsc",
}


def register_schemas(
    cfg: SchemaRegistrySettings,
    subject_to_file: Mapping[str, str] = SUBJECT_TO_FILE,
) -> None:
    """
    Register Avro schemas in Schema Registry.

    Args:
        cfg: Schema Registry config containing URL and schema directory.
        subject_to_file: Mapping of subject names to Avro schema filenames.

    Notes:
        - Safe to call repeatedly (Schema Registry is idempotent).
        - Only RetailEvent schema is required for this generator service.
    """
    client = SchemaRegistryClient({"url": cfg.url})

    for subject, filename in subject_to_file.items():
        path = os.path.join(cfg.schema_dir, filename)

        try:
            with open(path, encoding="utf-8") as f:
                schema_str = f.read()
        except FileNotFoundError:
            logger.error(
                "Schema file missing; cannot register subject.",
                extra={"subject": subject, "file": filename},
            )
            continue
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "Unexpected error reading Avro schema file.",
                extra={"subject": subject, "error": str(exc)},
            )
            continue

        schema = Schema(schema_str, "AVRO")

        try:
            version = client.register_schema(subject, schema)
            logger.info(
                "Schema registered successfully.",
                extra={"subject": subject, "version": version},
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "Schema registration failed; subject may already exist.",
                extra={"subject": subject, "error": str(exc)},
            )

    logger.info("Schema registration completed.")
