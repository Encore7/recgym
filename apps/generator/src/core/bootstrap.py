"""
Bootstraps the generator service:

- Initializes observability (logs, metrics, traces)
- Registers Avro schemas
- Ensures Kafka topics
- Creates event generator instance
"""

import logging
from typing import Final

from core.config import get_settings
from data.generator import EventGenerator
from data.schemas import register_schemas
from data.topics import ensure_topics
from models.generator_config import GeneratorConfig

from libs.observability.instrumentation import init_observability


def bootstrap() -> EventGenerator:
    """
    Initialize observability, schemas, topics, and event

    Returns:
        EventGenerator: A ready-to-use generator instance.
    """
    settings = get_settings()
    log: Final = logging.getLogger("bootstrap")

    log.info("Starting generator service bootstrap")

    # 1. Observability
    init_observability()
    log.info("Observability initialized")

    # 2. Schema registry
    register_schemas(settings.schema_config())
    log.info("Schemas registered")

    # 3. Kafka topics
    ensure_topics(settings.kafka_config())
    log.info("Topics ensured")

    # 4. Event generator
    generator = EventGenerator(GeneratorConfig())
    log.info("Event generator ready")

    return generator
