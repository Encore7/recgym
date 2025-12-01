"""
Realtime User Feature Job
=========================

Consumes raw events from Kafka and computes ~12 realtime user features:

    1. rt_user_last_event_ts_ms
    2. rt_user_time_since_last_event_sec
    3. rt_user_rolling_view_count_5m
    4. rt_user_rolling_cart_count_5m
    5. rt_user_rolling_purchase_count_5m
    6. rt_user_current_session_length
    7. rt_user_last_device
    8. rt_user_last_page_type
    9. rt_user_last_item_id
    10. rt_user_last_price
    11. rt_user_event_rate_per_min_5m
    12. rt_user_interaction_streak

Flink → compute → RedisFeatureWriter → Redis

This job runs indefinitely and updates Redis in near-real-time.
"""

from __future__ import annotations

from typing import Dict, Any

from pyflink.table import (
    EnvironmentSettings,
    TableEnvironment,
    Schema,
    DataTypes,
)

from libs.observability import get_logger, get_tracer
from realtime.infra.kafka import RealtimeKafkaConfig, build_consumer_properties
from realtime.infra.redis_writer import RedisFeatureWriter
from realtime.infra.utils import now_ms


logger = get_logger("rt.user_features_job")
tracer = get_tracer("rt.user_features_job")


# -----------------------------
#  MAIN DRIVER
# -----------------------------

def main() -> None:
    """
    Entry point for realtime user feature job.
    """
    logger.info("Starting realtime user feature job.")

    kafka_cfg = RealtimeKafkaConfig.from_app_config()
    kafka_props = build_consumer_properties(kafka_cfg, group_suffix="user-rt")

    # ------------------
    # Flink environment
    # ------------------
    settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(settings)

    t_env.create_temporary_table(
        "raw_events",
        _build_kafka_source(kafka_cfg, kafka_props),
    )

    # Read table
    events = t_env.from_path("raw_events")

    # Assign watermarks (Flink requirement)
    events = events.assign_watermarks("ts - INTERVAL '10' SECOND")

    # -----------------------
    # Windowed user features
    # -----------------------
    user_features = t_env.sql_query(
        """
        SELECT
            user_id,

            /* 1: Last timestamp */
            UNIX_TIMESTAMP(MAX(ts)) * 1000 AS rt_user_last_event_ts_ms,

            /* 2: Time since last event (approx real-time) */
            (UNIX_TIMESTAMP() * 1000 - UNIX_TIMESTAMP(MAX(ts)) * 1000) / 1000
                AS rt_user_time_since_last_event_sec,

            /* 5-minute rolling counts */
            SUM(CASE WHEN event_type='view' THEN 1 ELSE 0 END) AS rt_user_rolling_view_count_5m,
            SUM(CASE WHEN event_type='add_to_cart' THEN 1 ELSE 0 END) AS rt_user_rolling_cart_count_5m,
            SUM(CASE WHEN event_type='purchase' THEN 1 ELSE 0 END) AS rt_user_rolling_purchase_count_5m,

            /* event rate */
            COUNT(*) / 5.0 AS rt_user_event_rate_per_min_5m,

            /* last values via LAST_VALUE() */
            LAST_VALUE(device) AS rt_user_last_device,
            LAST_VALUE(page) AS rt_user_last_page_type,
            LAST_VALUE(item_id) AS rt_user_last_item_id,
            LAST_VALUE(price) AS rt_user_last_price,

            /* session length (simple approximation) */
            COUNT(*) AS rt_user_current_session_length,

            /* user streak */
            SUM(CASE WHEN event_type IN ('view','add_to_cart','purchase')
                     THEN 1 ELSE 0 END)
                AS rt_user_interaction_streak

        FROM TABLE(
            TUMBLE(TABLE raw_events, DESCRIPTOR(ts), INTERVAL '5' MINUTES)
        )
        GROUP BY user_id, window_start, window_end
        """
    )

    # -----------------------
    # Write to Redis
    # -----------------------
    redis_writer = RedisFeatureWriter()

    with tracer.start_as_current_span("user_features_write_loop"):
        results = user_features.execute().collect()

        for row in results:
            features = _row_to_dict(row)
            user_id = features.pop("user_id")

            redis_writer.write_user_features(user_id, features)

            logger.debug(
                "Updated realtime user features.",
                extra={"user_id": user_id, "features": features},
            )


def _row_to_dict(row) -> Dict[str, Any]:
    """
    Convert PyFlink Row to dict.
    """
    return {k: row[k] for k in row._fields}


def _build_kafka_source(
    kafka_cfg: RealtimeKafkaConfig, props: Dict[str, str]
) -> Dict[str, Any]:
    """
    Build the Kafka table config for Flink.
    """
    return {
        "connector": "kafka",
        "topic": kafka_cfg.topics.raw_events,
        "properties.bootstrap.servers": kafka_cfg.bootstrap_servers,
        "properties.group.id": props["group.id"],
        "format": "json",
        "json.timestamp-format.standard": "ISO-8601",
    }


if __name__ == "__main__":
    main()
