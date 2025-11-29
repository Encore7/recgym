"""
Silver layer job: derive silver_items from silver_events.

Responsibilities:
- Read silver_events.
- Build per-item attributes:
  - category_path (first non-null)
  - base_price (avg observed price)
  - created_ts (first seen)
  - synthetic title/description/brand for demo
"""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame, functions as F

from batch.config import get_batch_paths
from batch.utils import get_spark_session

logger = logging.getLogger(__name__)


def _build_silver_items(events: DataFrame) -> DataFrame:
    """
    Build silver_items table from event-level data.

    Output schema:
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


def main() -> None:
    """
    Entrypoint: run Silver items job.
    """
    paths = get_batch_paths()
    spark = get_spark_session("silver-items")

    logger.info("Reading Silver events.", extra={"path": paths.silver_events})
    events_df = spark.read.parquet(paths.silver_events)

    items_df = _build_silver_items(events_df)

    logger.info(
        "Writing Silver items.",
        extra={"output_path": paths.silver_items},
    )
    items_df.write.mode("overwrite").parquet(paths.silver_items)

    logger.info("Silver items job completed.")
    spark.stop()


if __name__ == "__main__":
    main()
