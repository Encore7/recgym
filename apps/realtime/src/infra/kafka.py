"""
Kafka helper utilities for realtime Flink jobs.

This module provides:
- A small typed wrapper around global KafkaConfig (from libs.config)
- Helpers to build Kafka consumer property dicts for Flink sources
- Topic accessors for reuse across realtime jobs

These helpers are intentionally lightweight so they can be imported
from PyFlink jobs without pulling in unnecessary dependencies.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict

from libs.config import AppConfig
from libs.observability import get_logger, get_tracer


logger = get_logger("realtime.kafka")
tracer = get_tracer("realtime.kafka")


@dataclass(frozen=True)
class RealtimeKafkaTopics:
    """
    Logical topic names used by realtime jobs.

    These are derived from the global KafkaConfig so that:
    - generator / ingestion / batch / realtime all stay consistent
    - we can change topic names via .env without code changes
    """

    raw_events: str
    user_feature_topic: str
    item_feature_topic: str


@dataclass(frozen=True)
class RealtimeKafkaConfig:
    """
    Snapshot of Kafka configuration used by realtime jobs.

    This is a thin wrapper around AppConfig.kafka, so that
    jobs don't reach directly into global config everywhere.
    """

    bootstrap_servers: str
    group_id_prefix: str
    topics: RealtimeKafkaTopics

    @classmethod
    def from_app_config(cls) -> "RealtimeKafkaConfig":
        """
        Build a RealtimeKafkaConfig from global AppConfig.
        """
        cfg = AppConfig.load().kafka

        topics = RealtimeKafkaTopics(
            raw_events=cfg.input_topic,
            user_feature_topic=cfg.user_feature_topic,
            item_feature_topic=cfg.item_feature_topic,
        )

        # Group ID prefix can be overridden later if needed (e.g. per-job).
        group_id_prefix = cfg.consumer_group or "recgym-realtime"

        return cls(
            bootstrap_servers=cfg.bootstrap_servers,
            group_id_prefix=group_id_prefix,
            topics=topics,
        )


def build_consumer_properties(
    kafka_cfg: RealtimeKafkaConfig,
    group_suffix: str,
) -> Dict[str, str]:
    """
    Build Kafka consumer properties for a realtime Flink job.

    These properties are passed into Flink's Kafka source
    (e.g. via Table API / DataStream API).

    Args:
        kafka_cfg: RealtimeKafkaConfig snapshot.
        group_suffix: Logical suffix to make consumer groups distinct
            per job, e.g. "user-features" / "item-features".

    Returns:
        Dictionary of Kafka consumer properties.
    """
    group_id = f"{kafka_cfg.group_id_prefix}-{group_suffix}"

    with tracer.start_as_current_span("build_consumer_properties") as span:
        span.set_attribute("kafka.bootstrap_servers", kafka_cfg.bootstrap_servers)
        span.set_attribute("kafka.group_id", group_id)
        logger.info(
            "Building Kafka consumer properties for realtime job.",
            extra={
                "bootstrap_servers": kafka_cfg.bootstrap_servers,
                "group_id": group_id,
            },
        )

        props: Dict[str, str] = {
            "bootstrap.servers": kafka_cfg.bootstrap_servers,
            "group.id": group_id,
            # Realtime jobs usually want latest; for backfill we can override
            "auto.offset.reset": "latest",
            "enable.auto.commit": "true",
            # Additional props can be added here as needed.
        }

        return props
