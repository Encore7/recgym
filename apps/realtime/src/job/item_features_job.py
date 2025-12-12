"""
Realtime Item Feature Job
=========================

Consumes raw events and computes 8 realtime item features:

    1. rt_item_last_event_ts_ms
    2. rt_item_time_since_last_event_sec
    3. rt_item_rolling_view_count_5m
    4. rt_item_rolling_cart_count_5m
    5. rt_item_rolling_purchase_count_5m
    6. rt_item_event_rate_per_min_5m
    7. rt_item_recent_price_avg_5m
    8. rt_item_popularity_delta_vs_30d  (requires Feast lookup)

These are then written to Redis via RedisFeatureWriter.
"""

from __future__ import annotations

from typing import Dict, Any

from pyflink.table import (
    EnvironmentSettings,
    TableEnvironment,
)

from libs.observability import get_logger, get_tracer
from realtime.infra.kafka import RealtimeKafkaConfig, build_consumer_properties
from realtime.infra.redis_writer import RedisFeatureWriter


logger = get_logger("rt.item_features_job")
tracer = get_tracer("rt.item_features_job")


def main() -> None:
    """
    Entry point for realtime item feature job.
    """
    logger.info("Starting realtime item feature job.")

    kafka_cfg = RealtimeKafkaConfig.from_app_config()
    kafka_props = build_consumer_properties(kafka_cfg, "item-rt")

    settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(settings)

    t_env.create_temporary_table("raw_events", _build_kafka_source(kafka_cfg, kafka_props))

    events = t_env.from_path("raw_events")
    events = events.assign_watermarks("ts - INTERVAL '10' SECOND")

    # -----------------------
    # Windowed item features
    # -----------------------
    item_features = t_env.sql_query(
        """
        SELECT
            item_id,

            /* timestamps */
            UNIX_TIMESTAMP(MAX(ts)) * 1000 AS rt_item_last_event_ts_ms,

            (UNIX_TIMESTAMP() * 1000 - UNIX_TIMESTAMP(MAX(ts)) * 1000) / 1000
                AS rt_item_time_since_last_event_sec,

            /* rolling counts */
            SUM(CASE WHEN event_type='view' THEN 1 ELSE 0 END) AS rt_item_rolling_view_count_5m,
            SUM(CASE WHEN event_type='add_to_cart' THEN 1 ELSE 0 END) AS rt_item_rolling_cart_count_5m,
            SUM(CASE WHEN event_type='purchase' THEN 1 ELSE 0 END) AS rt_item_rolling_purchase_count_5m,

            COUNT(*) / 5.0 AS rt_item_event_rate_per_min_5m,

            /* avg price */
            AVG(price) AS rt_item_recent_price_avg_5m,

            /* placeholder for Feast lookup */
            0.0 AS rt_item_popularity_delta_vs_30d

        FROM TABLE(
            TUMBLE(TABLE raw_events, DESCRIPTOR(ts), INTERVAL '5' MINUTES)
        )
        GROUP BY item_id, window_start, window_end
        """
    )

    redis_writer = RedisFeatureWriter()

    with tracer.start_as_current_span("item_features_write_loop"):
        results = item_features.execute().collect()

        for row in results:
            features = _row_to_dict(row)
            item_id = features.pop("item_id")

            redis_writer.write_item_features(item_id, features)

            logger.debug(
                "Updated realtime item features.",
                extra={"item_id": item_id, "features": features},
            )


def _row_to_dict(row) -> Dict[str, Any]:
    return {k: row[k] for k in row._fields}


def _build_kafka_source(
    kafka_cfg: RealtimeKafkaConfig, props: Dict[str, str]
) -> Dict[str, str]:
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
