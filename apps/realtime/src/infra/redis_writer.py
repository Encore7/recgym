"""
Redis writer for realtime features.

This module encapsulates:
- Connection to Redis using global RedisConfig
- Structured logging and tracing for writes
- Basic OpenTelemetry metrics for write success/failure/latency

The intended usage from a PyFlink job is:

    writer = RedisFeatureWriter()
    writer.write_user_features(user_id, feature_dict)
"""

from __future__ import annotations

import json
import time
from typing import Any, Dict, Mapping, Optional

import redis

from libs.config import AppConfig
from libs.observability import get_logger, get_tracer
from libs.observability.metrics import get_meter

from realtime.infra.utils import user_key, item_key, now_ms


logger = get_logger("realtime.redis_writer")
tracer = get_tracer("realtime.redis_writer")

_meter = get_meter()
_redis_write_success = _meter.create_counter(
    name="realtime_redis_writes_success",
    description="Count of successful Redis writes for realtime features.",
    unit="1",
)
_redis_write_failure = _meter.create_counter(
    name="realtime_redis_writes_failure",
    description="Count of failed Redis writes for realtime features.",
    unit="1",
)
_redis_write_latency_ms = _meter.create_histogram(
    name="realtime_redis_write_latency_ms",
    description="Latency of Redis writes for realtime features (ms).",
    unit="ms",
)


class RedisFeatureWriter:
    """
    Thin wrapper around redis-py for realtime feature writes.

    Responsibilities:
    - Initialize a Redis client from global AppConfig.redis
    - Provide typed helpers to write user/item feature payloads
    - Attach a freshness timestamp, if not already present
    - Emit logs, traces, and metrics around write operations
    """

    def __init__(self) -> None:
        cfg = AppConfig.load().redis

        self._ttl_sec: int = cfg.ttl_sec
        self._client = redis.Redis(
            host=cfg.host,
            port=cfg.port,
            db=cfg.db,
            decode_responses=True,
        )

        logger.info(
            "Initialized RedisFeatureWriter.",
            extra={
                "host": cfg.host,
                "port": cfg.port,
                "db": cfg.db,
                "ttl_sec": self._ttl_sec,
            },
        )

    @staticmethod
    def _prepare_payload(features: Mapping[str, Any]) -> str:
        """
        Ensure the payload has a freshness timestamp and JSON-encode it.

        Args:
            features: Mapping of feature names → values.

        Returns:
            JSON string ready to store in Redis.
        """
        payload: Dict[str, Any] = dict(features)
        payload.setdefault("rt_updated_at_ms", now_ms())
        return json.dumps(payload)

    def _write(
        self,
        key: str,
        features: Mapping[str, Any],
        *,
        ttl_override_sec: Optional[int] = None,
    ) -> None:
        """
        Core write helper with tracing + metrics.

        Args:
            key: Redis key to write.
            features: Feature mapping.
            ttl_override_sec: Optional override TTL in seconds.
        """
        ttl = ttl_override_sec if ttl_override_sec is not None else self._ttl_sec
        body = self._prepare_payload(features)

        with tracer.start_as_current_span("redis_write") as span:
            span.set_attribute("redis.key", key)
            span.set_attribute("redis.ttl_sec", ttl)

            start = time.perf_counter()
            try:
                # Using SETEX to set value + TTL atomically
                self._client.setex(name=key, time=ttl, value=body)
                duration_ms = (time.perf_counter() - start) * 1000
                _redis_write_success.add(1)
                _redis_write_latency_ms.record(duration_ms)

                logger.debug(
                    "Redis write success.",
                    extra={"key": key, "ttl_sec": ttl, "latency_ms": duration_ms},
                )
            except Exception as exc:  # noqa: BLE001
                duration_ms = (time.perf_counter() - start) * 1000
                _redis_write_failure.add(1)
                _redis_write_latency_ms.record(duration_ms)

                logger.exception(
                    "Redis write failure.",
                    extra={"key": key, "ttl_sec": ttl, "latency_ms": duration_ms},
                )
                # For realtime we typically do *not* raise; we just log & move on.

    # Public API

    def write_user_features(
        self,
        user_id: str,
        features: Mapping[str, Any],
        *,
        ttl_override_sec: Optional[int] = None,
    ) -> None:
        """
        Store realtime user features under the canonical Redis key.

        Args:
            user_id: User identifier.
            features: Feature mapping (e.g. 10–15 realtime user features).
            ttl_override_sec: Optional TTL override in seconds.
        """
        key = user_key(user_id)
        self._write(key, features, ttl_override_sec=ttl_override_sec)

    def write_item_features(
        self,
        item_id: str,
        features: Mapping[str, Any],
        *,
        ttl_override_sec: Optional[int] = None,
    ) -> None:
        """
        Store realtime item features under the canonical Redis key.

        Args:
            item_id: Item identifier.
            features: Feature mapping (e.g. RT popularity, last price).
            ttl_override_sec: Optional TTL override in seconds.
        """
        key = item_key(item_id)
        self._write(key, features, ttl_override_sec=ttl_override_sec)
