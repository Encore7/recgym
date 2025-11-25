"""
Global configuration system for all recgym services.

Strong typing + .env loading + nested config.
"""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path

from pydantic import Field, ValidationError
from pydantic_settings import BaseSettings, SettingsConfigDict

# Root of the repository
PROJECT_ROOT: Path = Path(__file__).resolve().parents[1]
DEFAULT_ENV_PATH: Path = PROJECT_ROOT / ".env"


class KafkaConfig(BaseSettings):
    """Kafka configuration for producers and consumers."""

    bootstrap_servers: str = Field(default="kafka:9092")
    schema_registry_url: str = Field(default="http://schema-registry:8081")
    input_topic: str = Field(default="events_raw")
    output_topic: str = Field(default="raw-events")
    consumer_group: str = Field(default="recgym-consumer")

    model_config = SettingsConfigDict(extra="ignore")


class S3Config(BaseSettings):
    """S3/MinIO configuration."""

    bucket: str = Field(default="recgym-raw")
    endpoint_url: str = Field(default="http://minio:9000")
    access_key: str = Field(default="minioadmin")
    secret_key: str = Field(default="minioadmin")

    model_config = SettingsConfigDict(extra="ignore")


class OTELConfig(BaseSettings):
    """OpenTelemetry configuration shared across all services."""

    service_name: str = Field(default="recgym-service")
    otlp_endpoint: str = Field(default="http://otel-collector:4317")
    resource_attributes: str = Field(default="deployment.environment=local")

    model_config = SettingsConfigDict(extra="ignore")


class ServiceConfig(BaseSettings):
    """Generic service-level configuration."""

    log_level: str = Field(default="INFO")
    debug: bool = Field(default=False)
    environment: str = Field(default="local")

    model_config = SettingsConfigDict(extra="ignore")


class AppConfig(BaseSettings):
    """
    Unified configuration object combining all sub-configs.

    Use:
        config = AppConfig.load()
    """

    kafka: KafkaConfig = Field(default_factory=KafkaConfig)
    s3: S3Config = Field(default_factory=S3Config)
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
