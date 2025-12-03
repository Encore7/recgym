from __future__ import annotations

"""
Batch configuration helpers.

Provides:
- Canonical S3A paths for bronze / silver / gold layers based on global AppConfig.s3.
"""

from dataclasses import dataclass
from functools import lru_cache

from libs.config import AppConfig


@dataclass(frozen=True)
class BatchPaths:
    """
    Canonical S3 paths for batch layers.

    All paths use the same bucket from global S3 config.
    """

    bucket: str

    # BRONZE (Kafka Connect → MinIO)
    @property
    def bronze_raw(self) -> str:
        """
        Bronze raw events written by Kafka Connect S3 sink.

        Path convention (Connect topics.dir = raw/events):

            s3a://{bucket}/raw/events/...

        Kafka Connect will create time-based partitions under this prefix.
        """
        return f"s3a://{self.bucket}/raw/events"

    # SILVER
    @property
    def silver_events(self) -> str:
        """Silver canonical events table (matches feature_catalog silver_events.path)."""
        return f"s3a://{self.bucket}/silver/events"

    @property
    def silver_users(self) -> str:
        """Silver user table derived from events."""
        return f"s3a://{self.bucket}/silver/users"

    @property
    def silver_items(self) -> str:
        """Silver item table derived from events / catalog."""
        return f"s3a://{self.bucket}/silver/items"

    # GOLD
    @property
    def gold_user_features(self) -> str:
        return f"s3a://{self.bucket}/gold/user_features"

    @property
    def gold_item_features(self) -> str:
        return f"s3a://{self.bucket}/gold/item_features"

    @property
    def gold_cross_features(self) -> str:
        return f"s3a://{self.bucket}/gold/cross_features"


@lru_cache(maxsize=1)
def get_batch_paths() -> BatchPaths:
    """
    Resolve batch paths from global AppConfig.s3.

    Returns:
        BatchPaths: object with S3A URIs for bronze / silver / gold.
    """
    cfg = AppConfig.load()
    return BatchPaths(bucket=cfg.s3.bucket)
