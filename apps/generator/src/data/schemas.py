import os

from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from core.models import SchemaRegistryConfig
from observability.logging import logging


def register_schemas(cfg: SchemaRegistryConfig) -> None:
    client = SchemaRegistryClient({"url": cfg.url})
    logger = logging.getLogger(__name__)

    for subject, file in cfg.subjects.items():
        path = os.path.join(cfg.schema_dir, file)
        with open(path) as f:
            schema_str = f.read()

        schema = Schema(schema_str, "AVRO")
        try:
            version = client.register_schema(subject, schema)
            logger.info(
                "Registered schema", extra={"subject": subject, "version": version}
            )
        except Exception as e:
            logger.warning(
                "⚠️ Schema registration failed",
                extra={"subject": subject, "error": str(e)},
            )

    logger.info("Schema registration complete.")


if __name__ == "__main__":
    cfg = SchemaRegistryConfig()
    register_schemas(cfg)
