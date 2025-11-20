from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings


class IngestionSettings(BaseSettings):
    """
    Configuration for the Kafka → S3 ingestion service.
    """

    service_name: str = Field(default="recgym-ingestion")
    environment: str = Field(default="local")

    # Kafka
    kafka_bootstrap_servers: str = Field(default="kafka:9092")
    kafka_topic: str = Field(default="events_raw")
    kafka_group_id: str = Field(default="recgym-ingestion-group")

    # Schema registry
    schema_registry_url: str = Field(default="http://schema-registry:8081")
    schema_dir: str = Field(default="infra/schemas")

    # S3 / MinIO
    s3_endpoint_url: str = Field(default="http://minio:9000")
    s3_access_key: str = Field(default="admin")
    s3_secret_key: str = Field(default="password123")
    s3_bucket: str = Field(default="recgym-raw-events")
    s3_region: str = Field(default="us-east-1")

    # Ingestion behavior
    batch_size: int = Field(default=5_000)
    flush_interval_sec: float = Field(default=30.0)

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False


@lru_cache()
def get_settings() -> IngestionSettings:
    return IngestionSettings()
