from __future__ import annotations

"""
Silver layer job: derive silver_items from silver_events.

Responsibilities:
- Read silver_events (Delta).
- Build per-item attributes:
  - category_path (first non-null)
  - base_price (avg observed price)
  - created_ts (first seen)
  - synthetic title/description/brand for demo
- Write Delta table to silver/items.
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

logger: logging.Logger = get_logger("batch.silver.items")
tracer = get_tracer("recgym.batch.silver.items")
_meter = get_meter()
JOB_LATENCY_MS: Histogram = _meter.create_histogram(
    name="batch_silver_items_latency_ms",
    description="Latency of silver_items job (ms).",
    unit="ms",
)


def _build_silver_items(events: DataFrame) -> DataFrame:
    """
    Build silver_items table from event-level data.

    Output schema (aligned with feature_catalog):
        item_id: string
        title: string
        description: string
        category_path: array<string>
        brand: string
        base_price: double
        created_ts: timestamp
    """
    grouped = (
        events.groupBy("item_id")
        .agg(
            F.first("category_path", ignorenulls=True).alias("category_path"),
            F.avg("price").alias("base_price"),
            F.min("ts").alias("created_ts"),
        )
        .dropna(subset=["item_id"])
    )

    items_df = (
        grouped.withColumn("title", F.concat(F.lit("Item "), F.col("item_id")))
        .withColumn(
            "description",
            F.concat(
                F.lit("Synthetic description for item "),
                F.col("item_id"),
            ),
        )
        .withColumn("brand", F.lit("generic_brand"))
    )

    return items_df


def _run_job(spark: SparkSession) -> None:
    """
    Core job logic: read silver_events → write silver_items.
    """
    paths = get_batch_paths()

    logger.info("Reading Silver events (Delta).", extra={"path": paths.silver_events})
    events_df = spark.read.format("delta").load(paths.silver_events)

    items_df = _build_silver_items(events_df)

    logger.info(
        "Writing Silver items (Delta).",
        extra={"output_path": paths.silver_items},
    )
    items_df.write.format("delta").mode("overwrite").save(paths.silver_items)

    logger.info("Silver items job completed.")


def main() -> None:
    """
    Entrypoint: run Silver items job with tracing + metrics.
    """
    spark = get_spark_session("silver-items")
    start = time.perf_counter()

    with tracer.start_as_current_span("silver_items_job"):
        try:
            _run_job(spark)
        finally:
            latency_ms = (time.perf_counter() - start) * 1000.0
            JOB_LATENCY_MS.record(latency_ms)
            logger.info(
                "Silver items job latency recorded.",
                extra={"latency_ms": latency_ms},
            )
            spark.stop()


if __name__ == "__main__":
    main()
