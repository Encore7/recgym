"""
Kafka topic naming for the generator service.
"""

from enum import Enum


class KafkaTopics(str, Enum):
    """
    Logical Kafka topics used by the generator.

    Currently, generator only produces to the event stream topic.
    """

    RETAIL_EVENTS = "events_raw"
