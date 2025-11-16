import logging

from confluent_kafka.admin import AdminClient, NewTopic
from models.kafka_config import KafkaConfig


def ensure_topics(cfg: KafkaConfig) -> None:
    """
    Ensure that the configured Kafka topic exists.

    This function is idempotent:
    - If the topic exists → logs and returns.
    - If missing → creates the topic with the given partitions and replication factor.
    """
    logger = logging.getLogger(__name__)
    admin = AdminClient({"bootstrap.servers": cfg.bootstrap_servers})

    # Fetch current topics
    metadata = admin.list_topics(timeout=10)
    existing = set(metadata.topics.keys())

    if cfg.topic in existing:
        logger.info(
            "Kafka topic(s) already exist.",
            extra={"topics": sorted(existing)},
        )
        return

    # Topic needs to be created
    new_topic = NewTopic(
        cfg.topic,
        num_partitions=cfg.num_partitions,
        replication_factor=cfg.replication_factor,
    )

    logger.info(
        "Creating Kafka topic",
        extra={
            "topic": cfg.topic,
            "partitions": cfg.num_partitions,
            "replication_factor": cfg.replication_factor,
        },
    )

    futures = admin.create_topics([new_topic])

    for topic, future in futures.items():
        try:
            future.result()
            logger.info("Created topic", extra={"topic": topic})
        except Exception as exc:
            # In local/dev it's okay if topic already exists due to race
            logger.error(
                "Topic creation failed",
                extra={"topic": topic, "error": str(exc)},
            )


if __name__ == "__main__":
    ensure_topics(KafkaConfig())
