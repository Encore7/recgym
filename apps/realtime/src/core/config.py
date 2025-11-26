"""
Realtime-specific configuration (PyFlink service).

This handles ONLY realtime / Flink concerns:
- parallelism, checkpoint dir
- realtime Kafka topics (input + feature output)
- Redis key prefixes (actual Redis host/port is global via AppConfig)

Environment variables (via RT__ prefix), e.g.:

    RT__PARALLELISM=1
    RT__CHECKPOINT_DIR=file:///tmp/flink-checkpoints

    RT__USER_FEATURE_TOPIC=user_features_rt
    RT__ITEM_FEATURE_TOPIC=item_features_rt

    RT__REDIS_USER_PREFIX=user_features_rt:
    RT__REDIS_ITEM_PREFIX=item_features_rt:
"""

from __future__ import annotations
from functools import lru_cache
from pydantic import Field
from pydantic_settings import BaseSettings


class RealtimeSettings(BaseSettings):
    parallelism: int = Field(default=1)
    checkpoint_dir: str = Field(default="file:///tmp/flink-checkpoints")

    # Topics
    kafka_input_topic: str = Field(default="events_raw")
    kafka_user_feature_topic: str = Field(default="user_features_rt")
    kafka_item_feature_topic: str = Field(default="item_features_rt")

    # Redis prefixes (rt-bridge will use this)
    redis_user_prefix: str = Field(default="user_features_rt:")
    redis_item_prefix: str = Field(default="item_features_rt:")

    class Config:
        env_prefix = "RT__"
        case_sensitive = False

    @classmethod
    @lru_cache(maxsize=1)
    def load(cls) -> "RealtimeSettings":
        return cls()
