"""
Kafka configuration model.
"""

from pydantic import BaseModel


class KafkaConfig(BaseModel):
    bootstrap_servers: str
    topic: str
    num_partitions: int
    replication_factor: int

    def dict_config(self) -> dict:
        """
        Convert Kafka config into Confluent-Kafka compatible dict.
        """
        return {"bootstrap.servers": self.bootstrap_servers}
