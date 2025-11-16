import logging
import os

from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from models.schema_config import SchemaRegistryConfig


def register_schemas(cfg: SchemaRegistryConfig) -> None:
    """
    Register Avro schemas in Schema Registry.

    This is safe to call repeatedly (Schema Registry is idempotent for registrations):
    - If a subject is already present with the same schema → existing version is reused.
    - If registration fails (e.g., registry down) → logs a warning and continues.
    """
    logger = logging.getLogger(__name__)
    client = SchemaRegistryClient({"url": cfg.url})

    for subject, filename in cfg.subjects.items():
        path = os.path.join(cfg.schema_dir, filename)

        try:
            with open(path, encoding="utf-8") as f:
                schema_str = f.read()
        except FileNotFoundError as exc:
            logger.warning(
                "Schema file not found; skipping",
                extra={"subject": subject, "path": path},
            )
            continue
        except Exception as exc:
            logger.error(
                "Error reading schema file; skipping",
                extra={"subject": subject, "path": path, "error": str(exc)},
            )
            continue

        schema = Schema(schema_str, "AVRO")
        try:
            version = client.register_schema(subject, schema)
            logger.info(
                "Registered schema",
                extra={"subject": subject, "version": version},
            )
        except Exception as exc:
            logger.warning(
                "Schema registration failed",
                extra={"subject": subject, "error": str(exc)},
            )

    logger.info("Schema registration complete.")


if __name__ == "__main__":
    register_schemas(SchemaRegistryConfig())
