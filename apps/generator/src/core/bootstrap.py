"""
Bootstrap wiring for the generator service.

Responsible for:
- Initializing configuration objects
- Constructing the KafkaEventProducer with dependencies
"""

from typing import Final

from apps.generator.src.core.config import (
    GeneratorSettings,
    KafkaSettings,
    SchemaRegistrySettings,
    get_generator_settings,
    get_kafka_settings,
    get_schema_registry_settings,
)
from apps.generator.src.infra.producer import KafkaEventProducer


def build_producer() -> KafkaEventProducer:
    """
    Build a fully wired KafkaEventProducer instance.

    Returns:
        KafkaEventProducer: Ready-to-run producer instance.
    """
    generator_cfg: Final[GeneratorSettings] = get_generator_settings()
    kafka_cfg: Final[KafkaSettings] = get_kafka_settings()
    schema_cfg: Final[SchemaRegistrySettings] = get_schema_registry_settings()

    return KafkaEventProducer(
        generator_cfg=generator_cfg,
        kafka_cfg=kafka_cfg,
        schema_cfg=schema_cfg,
    )
