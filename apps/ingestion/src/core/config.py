"""
Service-specific configuration for the ingestion control-plane.

This config controls:
- Kafka Connect REST endpoint
- Raw lakehouse path
- Table format (delta/iceberg/etc.)
- Poll interval for connector health checks
"""

from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings


class IngestionSettings(BaseSettings):
    """
    Ingestion control-plane settings.

    Values come from environment variables prefixed with `INGESTION__`.
    """

    connect_url: str = Field(
        default="http://kafka-connect:8083",
        description="Base URL for Kafka Connect REST API.",
    )

    connector_name: str = Field(
        default="recgym-s3-raw-events",
        description="Kafka Connect S3 sink connector name.",
    )

    raw_table_path: str = Field(
        default="s3://recgym-raw/raw/events",
        description="Lakehouse raw table path (MinIO-backed S3 URI).",
    )

    table_format: str = Field(
        default="delta",
        description="Lakehouse table format (e.g. 'delta').",
    )

    s3_prefix: str = Field(
        default="raw/events",
        description="Key prefix used by Kafka Connect S3 Sink for raw events.",
    )

    poll_interval_sec: float = Field(
        default=30.0,
        ge=1.0,
        description="Interval in seconds between connector health checks.",
    )

    class Config:
        env_prefix = "INGESTION__"
        case_sensitive = False


@lru_cache()
def get_ingestion_settings() -> IngestionSettings:
    """
    Cached accessor for IngestionSettings.

    Returns:
        IngestionSettings: validated ingestion configuration.
    """
    return IngestionSettings()
