"""
Schema Registry registration for Avro schemas.

Uses domain-level schema definitions and performs actual IO.
"""

import logging
import os
from typing import Mapping

from confluent_kafka.schema_registry import Schema, SchemaRegistryClient

from apps.generator.src.core.config import SchemaRegistrySettings
from apps.generator.src.domain.schemas import SUBJECT_TO_FILE


logger = logging.getLogger(__name__)


def register_schemas(
    cfg: SchemaRegistrySettings,
    subject_to_file: Mapping[str, str] = SUBJECT_TO_FILE,
) -> None:
    """
    Register Avro schemas for all known subjects.

    Args:
        cfg: Schema Registry configuration (URL + schema_dir).
        subject_to_file: Mapping from registry subject to Avro filename.

    Notes:
        Safe to call repeatedly (Schema Registry is idempotent).
    """
    client = SchemaRegistryClient({"url": cfg.url})

    for subject, filename in subject_to_file.items():
        path = os.path.join(cfg.schema_dir, filename)

        try:
            with open(path, encoding="utf-8") as f:
                schema_str = f.read()
        except FileNotFoundError as exc:
            logger.warning(
                "Schema file missing; skipping subject.",
                extra={"subject": subject, "file": filename},
            )
            continue
        except Exception as exc:
            logger.error(
                "Unexpected error reading Avro schema.",
                extra={"subject": subject, "error": str(exc)},
            )
            continue

        schema = Schema(schema_str, "AVRO")
        try:
            version = client.register_schema(subject, schema)
            logger.info(
                "Schema registered.",
                extra={"subject": subject, "version": version},
            )
        except Exception as exc:
            logger.warning(
                "Schema registration failed.",
                extra={"subject": subject, "error": str(exc)},
            )

    logger.info("Schema registration completed.")
