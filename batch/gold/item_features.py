"""
Gold layer: compute ~40 item features from silver_events + silver_items.
"""

from __future__ import annotations

import logging
from datetime import date

from pyspark.sql import DataFrame, functions as F

from batch.config import get_batch_paths
from batch.utils.spark import get_spark_session

logger = logging.getLogger(__name__)


def _read_silver(events_path: str, items_path: str, spark) -> tuple[DataFrame, DataFrame]:
    logger.info(
        "Reading Silver events and items.",
        extra={"events_path": events_path, "items_path": items_path},
    )
    ev = spark.read.parquet(events_path)
    items = spark.read.parquet(items_path)
    return ev, items


def _build_item_features(events: DataFrame, items: DataFrame) -> DataFrame:
    """
    Compute item-level stats & embeddings.
    """
    ev = events.withColumn("hour", F.hour("ts"))

    # base 30d aggregates
    agg = (
        ev.groupBy("item_id")
        .agg(
            F.sum(F.when(F.col("event_type") == "view", 1).otherwise(0)).alias(
                "item_views_30d"
            ),
            F.sum(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)).alias(
                "item_carts_30d"
            ),
            F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias(
                "item_purchases_30d"
            ),
            F.count("*").alias("total_events_30d"),
        )
    )

    # CTR (purchases / views) safe
    agg = agg.withColumn(
        "item_ctr_30d",
        F.when(F.col("item_views_30d") > 0,
               F.col("item_purchases_30d") / F.col("item_views_30d"))
        .otherwise(F.lit(0.0)),
    )

    # monetary stats
    monetary = (
        ev.groupBy("item_id")
        .agg(
            F.avg("price").alias("item_avg_price_30d"),
            F.stddev("price").alias("item_price_std_30d"),
            F.max("price").alias("item_max_price_30d"),
            F.min("price").alias("item_min_price_30d"),
        )
    )

    # distinct users
    disc = (
        ev.groupBy("item_id")
        .agg(
            F.countDistinct(
                F.when(F.col("event_type") == "view", F.col("user_id"))
            ).alias("item_distinct_users_viewed_30d"),
            F.countDistinct(
                F.when(F.col("event_type") == "purchase", F.col("user_id"))
            ).alias("item_distinct_users_purchased_30d"),
        )
    )

    # recency
    rec = (
        ev.groupBy("item_id")
        .agg(
            F.max("ts").alias("last_ts"),
            F.max(F.when(F.col("event_type") == "view", F.col("ts"))).alias(
                "last_view_ts"
            ),
            F.max(F.when(F.col("event_type") == "purchase", F.col("ts"))).alias(
                "last_purchase_ts"
            ),
        )
        .withColumn(
            "item_time_since_last_event_sec",
            F.unix_timestamp(F.current_timestamp()) - F.unix_timestamp("last_ts"),
        )
        .withColumn(
            "item_time_since_last_view_sec",
            F.unix_timestamp(F.current_timestamp()) - F.unix_timestamp("last_view_ts"),
        )
        .withColumn(
            "item_time_since_last_purchase_sec",
            F.unix_timestamp(F.current_timestamp())
            - F.unix_timestamp("last_purchase_ts"),
        )
    )

    # category + brand from silver_items
    cat_brand = (
        items.select(
            "item_id",
            "category_path",
            "brand",
            "base_price",
            "title",
            "description",
        )
        .withColumn("item_category_depth", F.size("category_path"))
        .withColumn(
            "item_is_discounted_flag",
            F.when(F.col("base_price") > 0.0, F.lit(1)).otherwise(F.lit(0)),
        )
    )

    # temporal shares
    tmp = (
        ev.groupBy("item_id")
        .agg(
            F.sum(F.when(F.col("hour").between(6, 11), 1).otherwise(0)).alias(
                "morning"
            ),
            F.sum(F.when(F.col("hour").between(18, 23), 1).otherwise(0)).alias(
                "evening"
            ),
            F.count("*").alias("total"),
        )
        .withColumn("item_morning_view_share_30d", F.col("morning") / F.col("total"))
        .withColumn("item_evening_view_share_30d", F.col("evening") / F.col("total"))
        .select(
            "item_id",
            "item_morning_view_share_30d",
            "item_evening_view_share_30d",
        )
    )

    # embeddings (dummy 10-dim)
    emb = items.select("item_id").dropDuplicates(["item_id"])
    emb = emb.withColumn(
        "item_behavior_emb", F.array([F.lit(0.2 * i) for i in range(1, 11)])
    )
    for i in range(10):
        emb = emb.withColumn(
            f"item_behavior_emb_{i+1}", F.col("item_behavior_emb")[i]
        )
    emb = emb.drop("item_behavior_emb")

    # item_log_popularity_score
    logpop = agg.select(
        "item_id",
        F.log(
            1
            + F.col("item_views_30d")
            + F.col("item_carts_30d")
            + F.col("item_purchases_30d")
        ).alias("item_log_popularity_score_30d"),
    )

    # join all
    df = (
        agg.join(monetary, "item_id", "left")
        .join(disc, "item_id", "left")
        .join(rec, "item_id", "left")
        .join(cat_brand, "item_id", "left")
        .join(tmp, "item_id", "left")
        .join(emb, "item_id", "left")
        .join(logpop, "item_id", "left")
    )

    return df


def main() -> None:
    paths = get_batch_paths()
    spark = get_spark_session("gold-item-features")
    logger.info("Starting Gold item features job.")

    events, items = _read_silver(paths.silver_events, paths.silver_items, spark)
    feats = _build_item_features(events, items)

    partition_date = date.today().isoformat()

    (
        feats.withColumn("date", F.lit(partition_date))
        .write.mode("overwrite")
        .partitionBy("date")
        .parquet(paths.gold_item_features)
    )

    logger.info("Gold item features job completed.")
    spark.stop()


if __name__ == "__main__":
    main()
