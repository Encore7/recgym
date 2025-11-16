"""
Application settings for the recgym-generator service.

Provides:
- Typed, validated environment configuration
- Accessors for Kafka + Schema Registry configs
- Singleton settings loader
"""

from functools import lru_cache

from models.kafka_config import KafkaConfig
from models.schema_config import SchemaRegistryConfig
from pydantic import Field
from pydantic_settings import BaseSettings


class AppSettings(BaseSettings):
    """
    Top-level configuration for the generator service.
    Loaded from environment variables and `.env` file.
    """

    # General metadata
    service_name: str = Field(default="recgym-generator")
    environment: str = Field(default="local")

    # Kafka settings
    kafka_bootstrap_servers: str = Field(default="kafka:9092")
    kafka_topic: str = Field(default="events_raw")
    kafka_partitions: int = Field(default=3)
    kafka_replication_factor: int = Field(default=1)

    # Schema registry
    schema_registry_url: str = Field(default="http://schema-registry:8081")
    schema_dir: str = Field(default="infra/schemas")

    # Observability
    otel_collector_endpoint: str = Field(default="http://otel-collector:4317")

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False

    def kafka_config(self) -> KafkaConfig:
        """Return a typed KafkaConfig object."""
        return KafkaConfig(
            bootstrap_servers=self.kafka_bootstrap_servers,
            topic=self.kafka_topic,
            num_partitions=self.kafka_partitions,
            replication_factor=self.kafka_replication_factor,
        )

    def schema_config(self) -> SchemaRegistryConfig:
        """Return a typed SchemaRegistryConfig object."""
        return SchemaRegistryConfig(
            url=self.schema_registry_url,
            schema_dir=self.schema_dir,
        )


@lru_cache()
def get_settings() -> AppSettings:
    """
    Cached singleton accessor for application settings.

    Returns:
        AppSettings: Typed settings model.
    """
    return AppSettings()
