import logging

from config import get_settings
from core.models import GeneratorConfig
from data.generator import EventGenerator
from data.schemas import register_schemas
from data.topics import ensure_topics
from observability.instrumentation import init_observability


def bootstrap() -> EventGenerator:
    """Initialize observability, topics, schemas, and return an event generator."""
    cfg = get_settings()
    log = logging.getLogger("bootstrap")

    log.info("Starting generator service bootstrap")

    # 1. Initialize observability (logs, metrics, traces)
    init_observability()
    log.info("Observability initialized")

    # 2. Register schemas (idempotent)
    schema_cfg = cfg.schema_config()
    register_schemas(schema_cfg)
    log.info("Schemas registered")

    # 3. Ensure Kafka topics exist (idempotent)
    kafka_cfg = cfg.kafka_config()
    ensure_topics(kafka_cfg)
    log.info("Topics ensured")

    # 4. Initialize event generator
    event_gen = EventGenerator(GeneratorConfig())
    log.info("Event generator ready")

    return event_gen
