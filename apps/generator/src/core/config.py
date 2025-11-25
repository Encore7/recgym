"""
Service-specific configuration for the generator.

This module ONLY handles:
- Synthetic event generation settings (user/item counts, session length, etc.)
- Kafka + Schema Registry runtime parameters needed by this service

It reads from the ROOT .env using namespaced keys:

    GENERATOR__USER_COUNT=100
    GENERATOR__ITEM_COUNT=1000
    GENERATOR__SESSION_LENGTH_MIN=2
    GENERATOR__SESSION_LENGTH_MAX=6
    GENERATOR__BASE_EVENT_DELAY_SEC=0.5
    GENERATOR__SCHEMA_DIR=/app/infra/schemas

    KAFKA__BOOTSTRAP_SERVERS=kafka:9092
    KAFKA__INPUT_TOPIC=events_raw
    KAFKA__SCHEMA_REGISTRY_URL=http://schema-registry:8081
"""

from functools import lru_cache
from typing import List, Optional

from pydantic import Field
from pydantic_settings import BaseSettings


class GeneratorSettings(BaseSettings):
    """
    Settings controlling synthetic event generation.

    Values come from environment variables prefixed with `GENERATOR__`.
    """

    user_count: int = Field(default=100, ge=1)
    item_count: int = Field(default=1000, ge=1)

    session_length_min: int = Field(default=2, ge=1)
    session_length_max: int = Field(default=6, ge=1)

    base_event_delay_sec: float = Field(default=0.5, ge=0.0)

    view_weight: float = Field(default=0.85, ge=0.0)
    add_to_cart_weight: float = Field(default=0.10, ge=0.0)
    purchase_weight: float = Field(default=0.05, ge=0.0)

    categories: List[str] = Field(
        default_factory=lambda: ["Electronics", "Fashion", "Books", "Home"]
    )
    referrers: List[Optional[str]] = Field(
        default_factory=lambda: ["campaign_1", "campaign_2", None]
    )

    price_min: float = Field(default=5.0)
    price_max: float = Field(default=200.0)

    schema_dir: str = Field(
        default="/app/infra/schemas",
        description="Directory containing Avro schema files for this service.",
    )

    class Config:
        env_prefix = "GENERATOR__"
        case_sensitive = False


class KafkaSettings(BaseSettings):
    """
    Kafka-specific settings for the generator producer.

    Values are read directly from environment variables:

    - KAFKA__BOOTSTRAP_SERVERS
    - KAFKA__INPUT_TOPIC
    """

    bootstrap_servers: str = Field(
        default="kafka:9092",
        description="Kafka bootstrap servers as host:port list.",
        env="KAFKA__BOOTSTRAP_SERVERS",
    )
    topic: str = Field(
        default="events_raw",
        description="Kafka topic to which synthetic events are produced.",
        env="KAFKA__INPUT_TOPIC",
    )

    class Config:
        case_sensitive = False


class SchemaRegistrySettings(BaseSettings):
    """
    Schema Registry settings for Avro-based serialization.

    Values are read from:

    - KAFKA__SCHEMA_REGISTRY_URL
    - GENERATOR__SCHEMA_DIR
    """

    url: str = Field(
        default="http://schema-registry:8081",
        env="KAFKA__SCHEMA_REGISTRY_URL",
        description="Schema Registry URL.",
    )
    schema_dir: str = Field(
        default="/app/infra/schemas",
        env="GENERATOR__SCHEMA_DIR",
        description="Directory containing Avro schema files.",
    )

    class Config:
        case_sensitive = False


@lru_cache()
def get_generator_settings() -> GeneratorSettings:
    """
    Cached accessor for GeneratorSettings.

    Returns:
        GeneratorSettings: validated generator configuration.
    """
    return GeneratorSettings()


@lru_cache()
def get_kafka_settings() -> KafkaSettings:
    """
    Cached accessor for KafkaSettings.
    """
    return KafkaSettings()


@lru_cache()
def get_schema_registry_settings() -> SchemaRegistrySettings:
    """
    Cached accessor for SchemaRegistrySettings.
    """
    return SchemaRegistrySettings()
