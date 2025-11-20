import logging
from typing import Final

from apps.ingestion.src.data.consumer import KafkaToS3IngestionService
from libs.observability.instrumentation import init_observability


def bootstrap() -> KafkaToS3IngestionService:
    """
    Initialize observability and return an ingestion service instance.
    """
    init_observability()
    log: Final = logging.getLogger("ingestion-bootstrap")
    log.info("Bootstrapping Kafka → S3 ingestion service")
    service = KafkaToS3IngestionService()
    log.info("Ingestion service ready")
    return service
