"""
Gold layer: compute ~40 user features from silver_events and silver_users.
"""

from __future__ import annotations

import logging
from datetime import date

from pyspark.sql import DataFrame, Window, functions as F

from batch.config import get_batch_paths
from batch.utils.spark import get_spark_session

logger = logging.getLogger(__name__)


def _read_silver(events_path: str, users_path: str, spark) -> DataFrame:
    """
    Read Silver events (and optionally users).

    Currently we only need events for feature computation.
    """
    logger.info(
        "Reading Silver events (and users).",
        extra={"events_path": events_path, "users_path": users_path},
    )
    events = spark.read.parquet(events_path)
    # users = spark.read.parquet(users_path)  # Keep if needed later.
    return events


def _build_user_features(events: DataFrame) -> DataFrame:
    """
    Compute user-level aggregates.

    Output columns SHOULD match the 40 user features in feature_catalog.yaml.
    """
    ev = events.withColumn("hour", F.hour("ts"))

    # === base counts ===
    base = (
        ev.groupBy("user_id")
        .agg(
            F.sum(F.when(F.col("event_type") == "view", 1).otherwise(0)).alias(
                "views_total"
            ),
            F.sum(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)).alias(
                "carts_total"
            ),
            F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias(
                "purchases_total"
            ),
        )
        .dropna(subset=["user_id"])
    )

    # === 30d-style aggregates ===
    agg_30d = (
        ev.groupBy("user_id")
        .agg(
            F.count("*").alias("user_total_events_30d"),
            F.sum(F.when(F.col("event_type") == "view", 1).otherwise(0)).alias(
                "user_views_30d"
            ),
            F.sum(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)).alias(
                "user_add_to_cart_30d"
            ),
            F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias(
                "user_purchases_30d"
            ),
            F.countDistinct(F.col("item_id")).alias(
                "user_distinct_items_viewed_30d"
            ),
            F.countDistinct(F.explode("category_path")).alias(
                "user_distinct_categories_viewed_30d"
            ),
        )
    )

    df = base.join(agg_30d, "user_id", "left")

    # conversion rates (safe)
    df = (
        df.withColumn(
            "user_view_to_cart_rate_30d",
            F.when(F.col("user_views_30d") > 0,
                   F.col("user_add_to_cart_30d") / F.col("user_views_30d"))
            .otherwise(F.lit(0.0)),
        )
        .withColumn(
            "user_cart_to_purchase_rate_30d",
            F.when(F.col("user_add_to_cart_30d") > 0,
                   F.col("user_purchases_30d") / F.col("user_add_to_cart_30d"))
            .otherwise(F.lit(0.0)),
        )
    )

    # monetary stats
    monetary = (
        ev.groupBy("user_id")
        .agg(
            F.avg("price").alias("user_avg_price_30d"),
            F.stddev("price").alias("user_price_std_30d"),
            F.max("price").alias("user_max_price_30d"),
            F.min("price").alias("user_min_price_30d"),
        )
        .dropna(subset=["user_id"])
    )
    df = df.join(monetary, "user_id", "left")

    # recency
    rec = (
        ev.groupBy("user_id")
        .agg(
            F.max("ts").alias("last_ts"),
            F.min("ts").alias("first_ts"),
            F.max(F.when(F.col("event_type") == "view", F.col("ts"))).alias(
                "last_view_ts"
            ),
            F.max(F.when(F.col("event_type") == "add_to_cart", F.col("ts"))).alias(
                "last_cart_ts"
            ),
            F.max(F.when(F.col("event_type") == "purchase", F.col("ts"))).alias(
                "last_purchase_ts"
            ),
        )
        .withColumn(
            "user_time_since_last_event_sec",
            F.unix_timestamp(F.current_timestamp()) - F.unix_timestamp("last_ts"),
        )
        .withColumn(
            "user_time_since_last_view_sec",
            F.unix_timestamp(F.current_timestamp()) - F.unix_timestamp("last_view_ts"),
        )
        .withColumn(
            "user_time_since_last_cart_sec",
            F.unix_timestamp(F.current_timestamp()) - F.unix_timestamp("last_cart_ts"),
        )
        .withColumn(
            "user_time_since_last_purchase_sec",
            F.unix_timestamp(F.current_timestamp())
            - F.unix_timestamp("last_purchase_ts"),
        )
        .withColumn(
            "user_days_since_first_event",
            (F.unix_timestamp(F.current_timestamp()) - F.unix_timestamp("first_ts"))
            / F.lit(86400.0),
        )
    )
    df = df.join(rec, "user_id", "left")

    # session metrics
    session_len = (
        ev.groupBy("user_id", "session_id")
        .agg(F.count("*").alias("session_len"))
        .groupBy("user_id")
        .agg(
            F.avg("session_len").alias("user_avg_session_length_events_30d"),
            F.expr("percentile(session_len, 0.5)").alias(
                "user_median_session_length_events_30d"
            ),
            F.count("*").alias("user_sessions_7d"),
        )
    )
    df = df.join(session_len, "user_id", "left")

    # device mix
    device = (
        ev.groupBy("user_id")
        .agg(
            F.sum(F.when(F.col("device") == "web", 1).otherwise(0)).alias("web"),
            F.sum(F.when(F.col("device") == "ios", 1).otherwise(0)).alias("ios"),
            F.sum(F.when(F.col("device") == "android", 1).otherwise(0)).alias(
                "android"
            ),
            F.count("*").alias("total"),
        )
        .withColumn("user_web_events_share_30d", F.col("web") / F.col("total"))
        .withColumn("user_ios_events_share_30d", F.col("ios") / F.col("total"))
        .withColumn("user_android_events_share_30d", F.col("android") / F.col("total"))
        .select(
            "user_id",
            "user_web_events_share_30d",
            "user_ios_events_share_30d",
            "user_android_events_share_30d",
        )
    )
    df = df.join(device, "user_id", "left")

    # temporal patterns
    tmp = (
        ev.groupBy("user_id")
        .agg(
            F.sum(F.when(F.col("hour").between(6, 11), 1).otherwise(0)).alias(
                "morning"
            ),
            F.sum(F.when(F.col("hour").between(18, 23), 1).otherwise(0)).alias(
                "evening"
            ),
            F.sum(F.when(F.col("hour").between(0, 5), 1).otherwise(0)).alias("night"),
            F.count("*").alias("total"),
        )
        .withColumn("user_morning_event_share_30d", F.col("morning") / F.col("total"))
        .withColumn("user_evening_event_share_30d", F.col("evening") / F.col("total"))
        .withColumn("user_night_event_share_30d", F.col("night") / F.col("total"))
        .select(
            "user_id",
            "user_morning_event_share_30d",
            "user_evening_event_share_30d",
            "user_night_event_share_30d",
        )
    )
    df = df.join(tmp, "user_id", "left")

    # top categories
    cat = (
        ev.withColumn("cat", F.explode("category_path"))
        .groupBy("user_id", "cat")
        .count()
        .withColumn(
            "rn",
            F.row_number().over(
                Window.partitionBy("user_id").orderBy(F.desc("count"))
            ),
        )
    )
    top3 = (
        cat.where(F.col("rn") <= 3)
        .groupBy("user_id")
        .pivot("rn")
        .agg(F.first("cat"))
        .withColumnRenamed("1", "user_top_category_1")
        .withColumnRenamed("2", "user_top_category_2")
        .withColumnRenamed("3", "user_top_category_3")
    )
    df = df.join(top3, "user_id", "left")

    # embeddings (dummy 10-dim behavior embedding)
    emb = df.select("user_id").dropDuplicates(["user_id"])
    emb = emb.withColumn(
        "user_behavior_emb", F.array([F.lit(0.1 * i) for i in range(1, 11)])
    )
    for i in range(10):
        emb = emb.withColumn(
            f"user_behavior_emb_{i+1}", F.col("user_behavior_emb")[i]
        )
    emb = emb.drop("user_behavior_emb")

    df = df.join(emb, "user_id", "left")

    return df


def main() -> None:
    paths = get_batch_paths()
    spark = get_spark_session("gold-user-features")
    logger.info("Starting Gold user features job.")

    events = _read_silver(paths.silver_events, paths.silver_users, spark)
    feats = _build_user_features(events)

    partition_date = date.today().isoformat()

    (
        feats.withColumn("date", F.lit(partition_date))
        .write.mode("overwrite")
        .partitionBy("date")
        .parquet(paths.gold_user_features)
    )

    logger.info("Gold user features job complete.")
    spark.stop()


if __name__ == "__main__":
    main()
