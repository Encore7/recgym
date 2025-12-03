from __future__ import annotations

"""
Silver layer job: bronze → silver_events.

Responsibilities:
- Read raw events from Bronze (Kafka Connect → MinIO).
- Normalize schema to canonical event schema (matching feature_catalog).
- Convert ts (millis) → timestamp (UTC).
- Basic cleaning: drop bad rows, ensure price > 0.
- Add event_date / event_hour partitions.
- Write Delta table to silver/events.
"""

import logging
import time
from typing import List

from opentelemetry.metrics import Histogram
from pyspark.sql import DataFrame, SparkSession, functions as F, types as T

from apps.batch.config import get_batch_paths
from apps.batch.utils.spark import get_spark_session
from libs.observability import get_logger
from libs.observability.metrics import get_meter
from libs.observability.tracing import get_tracer

logger: logging.Logger = get_logger("batch.silver.events")
tracer = get_tracer("recgym.batch.silver.events")
_meter = get_meter()
JOB_LATENCY_MS: Histogram = _meter.create_histogram(
    name="batch_silver_events_latency_ms",
    description="Latency of bronze→silver events job (ms).",
    unit="ms",
)


def _silver_schema() -> T.StructType:
    """
    Canonical Silver schema for events.

    Matches feature_catalog silver.silver_events.columns,
    but ts is a Spark TimestampType.
    """
    return T.StructType(
        [
            T.StructField("user_id", T.StringType(), nullable=False),
            T.StructField("session_id", T.StringType(), nullable=False),
            T.StructField("item_id", T.StringType(), nullable=False),
            T.StructField("event_type", T.StringType(), nullable=False),
            T.StructField("price", T.DoubleType(), nullable=False),
            T.StructField("category_path", T.ArrayType(T.StringType()), nullable=True),
            T.StructField("device", T.StringType(), nullable=False),
            T.StructField("page", T.StringType(), nullable=False),
            T.StructField("referrer", T.StringType(), nullable=True),
            T.StructField("ts", T.TimestampType(), nullable=False),
        ],
    )


def _normalize_ts(df: DataFrame) -> DataFrame:
    """
    Normalize ts column to timestamp (UTC).

    If ts is long/bigint in millis, convert → timestamp.
    """
    dtypes = dict(df.dtypes)
    if "ts" not in dtypes:
        raise RuntimeError("Bronze events missing 'ts' column.")

    if dtypes["ts"] in ("bigint", "long", "int"):
        df = df.withColumn("ts", (F.col("ts") / F.lit(1000.0)).cast("timestamp"))

    return df


def _sanitize_categoricals(df: DataFrame) -> DataFrame:
    """
    Normalize string categorical fields to lowercase.

    Ensures:
        - event_type ∈ {view, add_to_cart, purchase}
        - device ∈ {web, ios, android}
        - page ∈ {home, plp, pdp, cart}
    """
    df = (
        df.withColumn("event_type", F.lower(F.col("event_type")))
        .withColumn("device", F.lower(F.col("device")))
        .withColumn("page", F.lower(F.col("page")))
    )

    valid_event_types = ["view", "add_to_cart", "purchase"]
    valid_devices = ["web", "ios", "android"]
    valid_pages = ["home", "plp", "pdp", "cart"]

    df = df.filter(F.col("event_type").isin(valid_event_types))
    df = df.filter(F.col("device").isin(valid_devices))
    df = df.filter(F.col("page").isin(valid_pages))

    return df


def _to_silver(df: DataFrame) -> DataFrame:
    """
    Transform Bronze → Silver events.

    - Enforce required columns
    - Cast to canonical types
    - Basic filters (no null keys, price > 0)
    - Normalize categoricals
    - Add event_date / event_hour partitions
    - Deduplicate using full event identity
    """
    schema = _silver_schema()
    required_cols: List[str] = [field.name for field in schema]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise RuntimeError(f"Bronze events missing required columns: {missing}")

    df = _normalize_ts(df)

    silver = df.select(
        F.col("user_id").cast(T.StringType()),
        F.col("session_id").cast(T.StringType()),
        F.col("item_id").cast(T.StringType()),
        F.col("event_type").cast(T.StringType()),
        F.col("price").cast(T.DoubleType()),
        F.col("category_path").cast(T.ArrayType(T.StringType())),
        F.col("device").cast(T.StringType()),
        F.col("page").cast(T.StringType()),
        F.col("referrer").cast(T.StringType()),
        F.col("ts").cast(T.TimestampType()),
    )

    silver = silver.dropna(
        subset=["user_id", "session_id", "item_id", "event_type", "ts"],
    ).filter(F.col("price") > 0.0)

    silver = _sanitize_categoricals(silver)

    silver = silver.withColumn("event_date", F.to_date("ts")).withColumn(
        "event_hour",
        F.hour("ts"),
    )

    # Deduplicate with a robust key: all primary attributes + ts + price.
    silver = silver.dropDuplicates(
        [
            "user_id",
            "session_id",
            "item_id",
            "event_type",
            "ts",
            "price",
            "device",
            "page",
        ],
    )

    return silver


def _run_job(spark: SparkSession) -> None:
    """
    Core job logic (Spark in, Spark out) with no side effects.
    """
    paths = get_batch_paths()

    logger.info("Reading Bronze events.", extra={"path": paths.bronze_raw})
    bronze_df = (
        spark.read.option("recursiveFileLookup", "true").parquet(paths.bronze_raw)
    )

    # Basic pre-filter: ignore obviously broken records
    bronze_df = bronze_df.filter(F.col("user_id").isNotNull())

    silver_df = _to_silver(bronze_df)

    logger.info(
        "Writing Silver events (Delta).",
        extra={"output_path": paths.silver_events},
    )
    (
        silver_df.repartition("event_date", "event_hour")
        .write.format("delta")
        .mode("overwrite")
        .partitionBy("event_date", "event_hour")
        .save(paths.silver_events)
    )

    logger.info("Silver events job completed.")


def main() -> None:
    """
    Entrypoint: run Bronze → Silver events job.

    Wraps job with OTEL tracing + latency histogram.
    """
    spark = get_spark_session("silver-events")
    start = time.perf_counter()

    with tracer.start_as_current_span("silver_events_job"):
        try:
            _run_job(spark)
        finally:
            latency_ms = (time.perf_counter() - start) * 1000.0
            JOB_LATENCY_MS.record(latency_ms)
            logger.info(
                "Silver events job latency recorded.",
                extra={"latency_ms": latency_ms},
            )
            spark.stop()


if __name__ == "__main__":
    main()
