"""
Kafka connector DDLs for PyFlink Table API (Avro + Confluent Schema Registry).

This module returns Flink SQL DDL strings that:

- define `events_source` table over a Kafka topic with Avro+Schema Registry
- define `user_features_rt` sink table (Avro+Schema Registry)
- define `item_features_rt` sink table (Avro+Schema Registry)

We rely on:

- libs.config.AppConfig.kafka.bootstrap_servers
- libs.config.AppConfig.kafka.schema_registry_url

Connector JARs for:
- kafka
- avro-confluent

must be present on Flink's classpath.
"""

from __future__ import annotations

from libs.config import AppConfig
from apps.realtime.src.core.config import RealtimeSettings


def _bootstrap_and_registry(app_cfg: AppConfig) -> tuple[str, str]:
    """
    Helper to extract Kafka bootstrap + Schema Registry URL from AppConfig.
    """
    return app_cfg.kafka.bootstrap_servers, app_cfg.kafka.schema_registry_url


def build_events_source_ddl(cfg: RealtimeSettings, app_cfg: AppConfig) -> str:
    """
    Build CREATE TABLE DDL for the Kafka source providing RetailEvent stream.

    Assumptions:
      - Value is Avro encoded with Confluent Schema Registry
      - Subject is `<topic>-value` (default)
      - Avro schema for RetailEvent matches libs.models.events.RetailEvent
    """
    bootstrap, registry = _bootstrap_and_registry(app_cfg)

    return f"""
    CREATE TABLE IF NOT EXISTS events_source (
        user_id STRING,
        session_id STRING,
        item_id STRING,
        event_type STRING,
        price DOUBLE,
        category_path ARRAY<STRING>,
        device STRING,
        page STRING,
        referrer STRING,
        ts BIGINT,
        rowtime AS TO_TIMESTAMP_LTZ(ts, 3),
        WATERMARK FOR rowtime AS rowtime - INTERVAL '30' SECOND
    ) WITH (
        'connector' = 'kafka',
        'topic' = '{cfg.kafka_input_topic}',
        'properties.bootstrap.servers' = '{bootstrap}',
        'properties.group.id' = 'recgym-flink-realtime',
        'scan.startup.mode' = 'earliest-offset',

        -- Avro + Confluent Schema Registry
        'format' = 'avro-confluent',
        'avro-confluent.schema-registry.url' = '{registry}',
        'avro-confluent.subject' = '{cfg.kafka_input_topic}-value'
    )
    """


def build_user_features_sink_ddl(cfg: RealtimeSettings, app_cfg: AppConfig) -> str:
    """
    Build CREATE TABLE DDL for user-level realtime feature sink.

    Sink:
      - Kafka topic {cfg.kafka_user_feature_topic}
      - Avro+Schema Registry, subject `<topic>-value`
      - Schema derived from table columns below
    """
    bootstrap, registry = _bootstrap_and_registry(app_cfg)

    return f"""
    CREATE TABLE IF NOT EXISTS user_features_rt (
        user_id STRING,
        session_len INT,
        clicks_in_session INT,
        last_event_ts BIGINT
    ) WITH (
        'connector' = 'kafka',
        'topic' = '{cfg.kafka_user_feature_topic}',
        'properties.bootstrap.servers' = '{bootstrap}',

        'format' = 'avro-confluent',
        'avro-confluent.schema-registry.url' = '{registry}',
        'avro-confluent.subject' = '{cfg.kafka_user_feature_topic}-value'
    )
    """


def build_item_features_sink_ddl(cfg: RealtimeSettings, app_cfg: AppConfig) -> str:
    """
    Build CREATE TABLE DDL for item-level realtime feature sink.

    Sink:
      - Kafka topic {cfg.kafka_item_feature_topic}
      - Avro+Schema Registry, subject `<topic>-value`
      - Schema derived from table columns below
    """
    bootstrap, registry = _bootstrap_and_registry(app_cfg)

    return f"""
    CREATE TABLE IF NOT EXISTS item_features_rt (
        item_id STRING,
        views_5m INT,
        cart_5m INT,
        purchases_5m INT,
        avg_price_5m DOUBLE,
        ts BIGINT,
        category_hint STRING
    ) WITH (
        'connector' = 'kafka',
        'topic' = '{cfg.kafka_item_feature_topic}',
        'properties.bootstrap.servers' = '{bootstrap}',

        'format' = 'avro-confluent',
        'avro-confluent.schema-registry.url' = '{registry}',
        'avro-confluent.subject' = '{cfg.kafka_item_feature_topic}-value'
    )
    """
