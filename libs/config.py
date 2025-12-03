"""
Global configuration system for all recgym services.

Provides globally shared configuration:
- Kafka cluster settings
- S3/MinIO settings
- Redis online store settings
- OTEL settings
- Generic service-level runtime settings

Service-specific settings (generator, ingestion, realtime, API) live in
their own modules and must NOT be added here.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import Field, ValidationError
from pydantic_settings import BaseSettings, SettingsConfigDict

PROJECT_ROOT: Path = Path(__file__).resolve().parents[1]
DEFAULT_ENV_PATH: Path = PROJECT_ROOT / ".env"


class KafkaConfig(BaseSettings):
    """Kafka configuration for all services."""

    bootstrap_servers: str = Field(default="kafka:9092")
    schema_registry_url: str = Field(default="http://schema-registry:8081")

    raw_topic: str = Field(default="raw-events")
    consumer_group: str = Field(default="recgym-consumer")

    model_config = SettingsConfigDict(extra="ignore")


class S3Config(BaseSettings):
    """Shared S3/MinIO configuration."""

    bucket: str = Field(default="recgym-raw")
    endpoint_url: str = Field(default="http://minio:9000")
    access_key: str = Field(default="minioadmin")
    secret_key: str = Field(default="minioadmin")

    model_config = SettingsConfigDict(extra="ignore")


class RedisConfig(BaseSettings):
    """Redis online feature store settings."""

    host: str = Field(default="redis")
    port: int = Field(default=6379)
    db: int = Field(default=0)
    ttl_sec: int = Field(default=3600)

    user_prefix: str = Field(default="user_features_rt:")
    item_prefix: str = Field(default="item_features_rt:")

    model_config = SettingsConfigDict(extra="ignore")


class OTELConfig(BaseSettings):
    """OpenTelemetry configuration shared across services."""

    service_name: str = Field(default="recgym-service")
    otlp_endpoint: str = Field(default="http://otel-collector:4317")
    resource_attributes: str = Field(default="deployment.environment=local")

    model_config = SettingsConfigDict(extra="ignore")


class ServiceConfig(BaseSettings):
    """Generic service-level config."""

    log_level: str = Field(default="INFO")
    debug: bool = Field(default=False)
    environment: str = Field(default="local")

    model_config = SettingsConfigDict(extra="ignore")


class AppConfig(BaseSettings):
    """Root global configuration object."""

    kafka: KafkaConfig = Field(default_factory=KafkaConfig)
    s3: S3Config = Field(default_factory=S3Config)
    redis: RedisConfig = Field(default_factory=RedisConfig)
    otel: OTELConfig = Field(default_factory=OTELConfig)
    service: ServiceConfig = Field(default_factory=ServiceConfig)

    model_config = SettingsConfigDict(
        env_nested_delimiter="__",
        extra="ignore",
    )

    @classmethod
    @lru_cache(maxsize=1)
    def load(cls) -> "AppConfig":
        try:
            env_file = str(DEFAULT_ENV_PATH) if DEFAULT_ENV_PATH.exists() else None
            return cls(_env_file=env_file)
        except ValidationError as exc:
            raise RuntimeError("Invalid configuration values.") from exc
