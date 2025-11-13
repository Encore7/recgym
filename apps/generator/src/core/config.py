from functools import lru_cache

from models import KafkaConfig, SchemaRegistryConfig
from pydantic import BaseSettings, Field


class AppSettings(BaseSettings):
    """Top-level app settings for the generator service."""

    # General service metadata
    service_name: str = Field(default="recgym-generator")
    environment: str = Field(default="local")

    # Kafka / Schema Registry settings
    kafka_bootstrap_servers: str = Field(default="kafka:9092")
    kafka_topic: str = Field(default="events_raw")
    kafka_partitions: int = Field(default=3)
    kafka_replication_factor: int = Field(default=1)

    schema_registry_url: str = Field(default="http://schema-registry:8081")
    schema_dir: str = Field(default="infra/schemas")

    # Observability
    otel_collector_endpoint: str = Field(default="http://otel-collector:4317")

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False

    def kafka_config(self) -> KafkaConfig:
        return KafkaConfig(
            bootstrap_servers=self.kafka_bootstrap_servers,
            topic=self.kafka_topic,
            num_partitions=self.kafka_partitions,
            replication_factor=self.kafka_replication_factor,
        )

    def schema_config(self) -> SchemaRegistryConfig:
        return SchemaRegistryConfig(
            url=self.schema_registry_url,
            schema_dir=self.schema_dir,
        )


@lru_cache()
def get_settings() -> AppSettings:
    """Singleton accessor for settings (cached)."""
    return AppSettings()


if __name__ == "__main__":
    s = get_settings()
    print(s.model_dump_json(indent=2))
