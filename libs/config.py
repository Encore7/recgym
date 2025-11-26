"""
Global configuration system for all recgym services.

This module provides:
- Strongly typed Pydantic configuration models (Kafka, S3, Redis, OTEL, Service, Ingestion)
- Automatic loading from environment variables + .env file
- A single `AppConfig` object that every service should import

Usage:
    from libs.config import AppConfig
    config = AppConfig.load()
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import Field, ValidationError
from pydantic_settings import BaseSettings, SettingsConfigDict

# Root of the repository (used for .env autodiscovery)
PROJECT_ROOT: Path = Path(__file__).resolve().parents[1]
DEFAULT_ENV_PATH: Path = PROJECT_ROOT / ".env"


# KAFKA
class KafkaConfig(BaseSettings):
    """Kafka configuration for producers and consumers."""

    bootstrap_servers: str = Field(default="kafka:9092")
    schema_registry_url: str = Field(default="http://schema-registry:8081")

    # Topics (shared defaults)
    input_topic: str = Field(default="events_raw")
    output_topic: str = Field(default="raw-events")
    consumer_group: str = Field(default="recgym-consumer")

    # Realtime feature topics (used by realtime + rt-bridge + API)
    user_feature_topic: str = Field(default="user_features_rt")
    item_feature_topic: str = Field(default="item_features_rt")

    model_config = SettingsConfigDict(extra="ignore")


# S3 (MinIO)
class S3Config(BaseSettings):
    """S3/MinIO configuration used by ingestion pipelines."""

    bucket: str = Field(default="recgym-raw")
    endpoint_url: str = Field(default="http://minio:9000")
    access_key: str = Field(default="minioadmin")
    secret_key: str = Field(default="minioadmin")

    model_config = SettingsConfigDict(extra="ignore")


# REDIS (GLOBAL ONLINE STORE)
class RedisConfig(BaseSettings):
    """Global Redis online feature store settings."""

    host: str = Field(default="redis")
    port: int = Field(default=6379)
    db: int = Field(default=0)
    ttl_sec: int = Field(default=3600)

    # Key prefixes for online feature snapshots
    user_prefix: str = Field(default="user_features_rt:")
    item_prefix: str = Field(default="item_features_rt:")

    model_config = SettingsConfigDict(extra="ignore")


# OTEL
class OTELConfig(BaseSettings):
    """OpenTelemetry configuration shared across all services."""

    service_name: str = Field(default="recgym-service")
    otlp_endpoint: str = Field(default="http://otel-collector:4317")
    resource_attributes: str = Field(default="deployment.environment=local")

    model_config = SettingsConfigDict(extra="ignore")


# GENERIC SERVICE CONFIG
class ServiceConfig(BaseSettings):
    """
    Generic service-level configuration.

    Each service may override these via env vars.
    """

    log_level: str = Field(default="INFO")
    debug: bool = Field(default=False)
    environment: str = Field(default="local")

    model_config = SettingsConfigDict(extra="ignore")


# INGESTION SETTINGS
class IngestionConfig(BaseSettings):
    """
    Ingestion-specific configuration for Kafka → S3 sink.
    """

    batch_size: int = Field(default=5_000)
    flush_interval_sec: float = Field(default=30.0)

    model_config = SettingsConfigDict(extra="ignore")


# ROOT CONFIG OBJECT
class AppConfig(BaseSettings):
    """
    Unified configuration object combining all sub-configs.

    Services must call:
        config = AppConfig.load()
    """

    kafka: KafkaConfig = Field(default_factory=KafkaConfig)
    s3: S3Config = Field(default_factory=S3Config)
    redis: RedisConfig = Field(default_factory=RedisConfig)
    otel: OTELConfig = Field(default_factory=OTELConfig)
    service: ServiceConfig = Field(default_factory=ServiceConfig)
    ingestion: IngestionConfig = Field(default_factory=IngestionConfig)

    model_config = SettingsConfigDict(
        env_nested_delimiter="__",
        extra="ignore",
    )

    # Singleton loader
    @classmethod
    @lru_cache(maxsize=1)
    def load(cls) -> "AppConfig":
        """
        Load configuration from:
        - root `.env`
        - environment variables
        - defaults
        """
        try:
            env_file = str(DEFAULT_ENV_PATH) if DEFAULT_ENV_PATH.exists() else None
            return cls(_env_file=env_file)
        except ValidationError as exc:
            raise RuntimeError("Invalid configuration values.") from exc
