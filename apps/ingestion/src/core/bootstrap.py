"""
Bootstrap for the Kafka → S3 ingestion service.

Responsibilities:
- Initialize observability (logs, traces, metrics)
- Construct the KafkaToS3IngestionService instance
"""

from typing import Final

from libs.observability import get_logger, init_observability
from ingestion.src.data.consumer import KafkaToS3IngestionService


def bootstrap() -> KafkaToS3IngestionService:
    """
    Initialize observability and return a ready ingestion service.

    Returns:
        KafkaToS3IngestionService: Configured service instance.
    """
    init_observability(service_name="recgym-ingestion")
    log = get_logger("ingestion-bootstrap")
    log.info("Bootstrapping Kafka → S3 ingestion service")
    service = KafkaToS3IngestionService()
    log.info("Ingestion service initialized")
    return service
