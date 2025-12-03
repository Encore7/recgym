from __future__ import annotations

"""
Silver layer job: derive silver_users from silver_events.

Responsibilities:
- Read silver_events (Delta).
- Aggregate per-user activity:
  - first_seen_ts
  - last_seen_ts
  - total_events (extra, but harmless)
- Write Delta table to silver/users.
"""

import logging
import time

from opentelemetry.metrics import Histogram
from pyspark.sql import DataFrame, SparkSession, functions as F

from apps.batch.config import get_batch_paths
from apps.batch.utils.spark import get_spark_session
from libs.observability import get_logger
from libs.observability.metrics import get_meter
from libs.observability.tracing import get_tracer

logger: logging.Logger = get_logger("batch.silver.users")
tracer = get_tracer("recgym.batch.silver.users")
_meter = get_meter()
JOB_LATENCY_MS: Histogram = _meter.create_histogram(
    name="batch_silver_users_latency_ms",
    description="Latency of silver_users job (ms).",
    unit="ms",
)


def _build_silver_users(events: DataFrame) -> DataFrame:
    """
    Build silver_users table from event-level data.

    Output schema (superset of feature_catalog):
        user_id: string
        first_seen_ts: timestamp
        last_seen_ts: timestamp
        total_events: long
    """
    users_df = (
        events.groupBy("user_id")
        .agg(
            F.min("ts").alias("first_seen_ts"),
            F.max("ts").alias("last_seen_ts"),
            F.count("*").alias("total_events"),
        )
        .dropna(subset=["user_id"])
    )
    return users_df


def _run_job(spark: SparkSession) -> None:
    """
    Core job logic: read silver_events → write silver_users.
    """
    paths = get_batch_paths()

    logger.info("Reading Silver events (Delta).", extra={"path": paths.silver_events})
    events_df = spark.read.format("delta").load(paths.silver_events)

    users_df = _build_silver_users(events_df)

    logger.info(
        "Writing Silver users (Delta).",
        extra={"output_path": paths.silver_users},
    )
    users_df.write.format("delta").mode("overwrite").save(paths.silver_users)

    logger.info("Silver users job completed.")


def main() -> None:
    """
    Entrypoint: run Silver users job with tracing + metrics.
    """
    spark = get_spark_session("silver-users")
    start = time.perf_counter()

    with tracer.start_as_current_span("silver_users_job"):
        try:
            _run_job(spark)
        finally:
            latency_ms = (time.perf_counter() - start) * 1000.0
            JOB_LATENCY_MS.record(latency_ms)
            logger.info(
                "Silver users job latency recorded.",
                extra={"latency_ms": latency_ms},
            )
            spark.stop()


if __name__ == "__main__":
    main()
