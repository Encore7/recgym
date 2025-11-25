"""
Kafka topic management for the generator service.
"""

import logging

from confluent_kafka.admin import AdminClient, NewTopic

from apps.generator.src.core.config import KafkaSettings


logger = logging.getLogger(__name__)


def ensure_topic(cfg: KafkaSettings) -> None:
    """
    Ensure that the configured Kafka topic exists.

    Args:
        cfg: Kafka settings for the generator producer.

    Behavior:
        - If topic exists: logs and returns.
        - If missing: creates topic with default partitions/replication as
          defined in the Kafka cluster (we do not manage those per-service here).
    """
    admin = AdminClient({"bootstrap.servers": cfg.bootstrap_servers})

    metadata = admin.list_topics(timeout=10)
    existing = set(metadata.topics.keys())

    if cfg.topic in existing:
        logger.info(
            "Kafka topic already exists.",
            extra={"topic": cfg.topic},
        )
        return

    new_topic = NewTopic(cfg.topic, num_partitions=3, replication_factor=1)

    logger.info(
        "Creating Kafka topic.",
        extra={
            "topic": cfg.topic,
            "num_partitions": 3,
            "replication_factor": 1,
        },
    )

    futures = admin.create_topics([new_topic])

    for topic, future in futures.items():
        try:
            future.result()
            logger.info("Created Kafka topic.", extra={"topic": topic})
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "Topic creation failed.",
                extra={"topic": topic, "error": str(exc)},
            )
