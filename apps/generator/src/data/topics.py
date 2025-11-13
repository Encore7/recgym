from confluent_kafka.admin import AdminClient, NewTopic

from ..core.models import KafkaConfig
from ..observability.logging import logging


def ensure_topics(cfg: KafkaConfig) -> None:
    admin = AdminClient({"bootstrap.servers": cfg.bootstrap_servers})
    logger = logging.getLogger(__name__)

    existing = admin.list_topics(timeout=10).topics.keys()
    topics_to_create = []

    if cfg.topic not in existing:
        topics_to_create.append(
            NewTopic(
                cfg.topic,
                num_partitions=cfg.num_partitions,
                replication_factor=cfg.replication_factor,
            )
        )

    if not topics_to_create:
        logger.info("Kafka topic(s) already exist.", extra={"topics": list(existing)})
        return

    fs = admin.create_topics(topics_to_create)
    for topic, f in fs.items():
        try:
            f.result()
            logger.info("Created topic", extra={"topic": topic})
        except Exception as e:
            logger.error(
                "Topic creation failed", extra={"topic": topic, "error": str(e)}
            )


if __name__ == "__main__":
    cfg = KafkaConfig()
    ensure_topics(cfg)
