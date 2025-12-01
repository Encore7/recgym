"""
Utility helpers for realtime feature pipelines.

This module provides:
- Canonical Redis key builders for user/item realtime features
- Small time helpers that keep everything in UTC / epoch millis

These utilities are imported by PyFlink jobs and RT writers so
we keep them dependency-light and deterministic.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Final

from libs.config import AppConfig
from libs.observability import get_logger, get_tracer


logger = get_logger("realtime.utils")
tracer = get_tracer("realtime.utils")


def _redis_prefixes() -> tuple[str, str]:
    """
    Internal helper to read Redis key prefixes from global config.

    Returns:
        Tuple of (user_prefix, item_prefix).
    """
    cfg = AppConfig.load().redis
    return cfg.user_prefix, cfg.item_prefix


def user_key(user_id: str) -> str:
    """
    Build the canonical Redis key for a user's realtime features.

    Args:
        user_id: Logical user identifier.

    Returns:
        Key string, e.g. "user_features_rt:u_123".
    """
    with tracer.start_as_current_span("build_user_key") as span:
        span.set_attribute("realtime.user_id", user_id)
        user_prefix, _ = _redis_prefixes()
        key = f"{user_prefix}{user_id}"
        logger.debug("Built user feature key.", extra={"user_id": user_id, "key": key})
        return key


def item_key(item_id: str) -> str:
    """
    Build the canonical Redis key for an item's realtime features.

    Args:
        item_id: Logical item identifier.

    Returns:
        Key string, e.g. "item_features_rt:i_42".
    """
    with tracer.start_as_current_span("build_item_key") as span:
        span.set_attribute("realtime.item_id", item_id)
        _, item_prefix = _redis_prefixes()
        key = f"{item_prefix}{item_id}"
        logger.debug("Built item feature key.", extra={"item_id": item_id, "key": key})
        return key


def now_ms() -> int:
    """
    Current UTC time in epoch milliseconds.

    Useful when stamping realtime feature freshness timestamps.
    """
    ts = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
    logger.debug("Computed now_ms.", extra={"ts": ts})
    return ts
