"""
Gold layer: compute user-item cross features.

Cross features join:
- gold_user_features
- gold_item_features
- silver_events (for co-occurrence/recency/session context)
"""

from __future__ import annotations

import logging
from datetime import date

from pyspark.sql import DataFrame, Window, functions as F

from batch.config import get_batch_paths
from batch.utils.spark import get_spark_session

logger = logging.getLogger(__name__)


def _read_gold_and_silver(
    events_path: str,
    user_features_path: str,
    item_features_path: str,
    spark,
) -> tuple[DataFrame, DataFrame, DataFrame]:
    """
    Read Silver events + Gold user/item features.
    """
    logger.info(
        "Reading Silver events + Gold user/item features.",
        extra={
            "events_path": events_path,
            "user_features_path": user_features_path,
            "item_features_path": item_features_path,
        },
    )
    ev = spark.read.parquet(events_path)
    uf = spark.read.parquet(user_features_path)
    itf = spark.read.parquet(item_features_path)
    return ev, uf, itf


def _build_cross_features(ev: DataFrame, uf: DataFrame, itf: DataFrame) -> DataFrame:
    """
    Build user-item cross features.
    """
    # item meta
    item_meta = itf.select(
        "item_id", "item_avg_price_30d", "category_path", "brand"
    )

    # base event pairs
    base = (
        ev.select("user_id", "item_id", "ts", "event_type", "session_id")
        .dropna(subset=["user_id", "item_id"])
        .dropDuplicates(["user_id", "item_id", "ts"])
    )

    # recency per user-item
    rec = (
        base.groupBy("user_id", "item_id")
        .agg(F.max("ts").alias("last_ui_ts"))
        .withColumn(
            "ui_time_since_user_last_interaction_with_item_sec",
            F.unix_timestamp(F.current_timestamp()) - F.unix_timestamp("last_ui_ts"),
        )
    )

    # co-occurrence counts
    cooc = (
        base.groupBy("user_id", "item_id")
        .agg(
            F.sum(F.when(F.col("event_type") == "view", 1).otherwise(0)).alias(
                "ui_user_item_co_views_30d"
            ),
            F.sum(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)).alias(
                "ui_user_item_co_carts_30d"
            ),
            F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias(
                "ui_user_item_co_purchases_30d"
            ),
        )
    )

    # join user + item features
    joined = (
        base.select("user_id", "item_id")
        .dropDuplicates(["user_id", "item_id"])
        .join(uf, "user_id", "left")
        .join(item_meta, "item_id", "left")
        .join(itf.select("item_id", "item_behavior_emb_1"), "item_id", "left")
    )

    # category matches
    cat_match = (
        joined.withColumn(
            "ui_same_category_flag",
            F.when(
                F.array_contains(F.col("category_path"), F.col("user_top_category_1")),
                1,
            ).otherwise(0),
        )
        .withColumn(
            "ui_same_top_category_flag",
            F.when(
                F.col("user_top_category_1") == F.element_at("category_path", 1), 1
            ).otherwise(0),
        )
        .select(
            "user_id",
            "item_id",
            "ui_same_category_flag",
            "ui_same_top_category_flag",
        )
    )

    # price alignment (safe when stddev is null/0)
    price_align = joined.select(
        "user_id",
        "item_id",
        (F.col("item_avg_price_30d") - F.col("user_avg_price_30d")).alias(
            "ui_price_diff_from_user_avg"
        ),
        F.when(
            (F.col("user_price_std_30d").isNotNull())
            & (F.col("user_price_std_30d") > 0),
            (F.col("item_avg_price_30d") - F.col("user_avg_price_30d"))
            / F.col("user_price_std_30d"),
        )
        .otherwise(F.lit(0.0))
        .alias("ui_price_zscore_for_user"),
    )

    # similarity between embeddings (placeholder)
    sim = (
        joined.select("user_id", "item_id")
        .withColumn("ui_user_item_similarity_emb", F.lit(0.5))
    )

    # exploration score (placeholder)
    expl = (
        joined.select("user_id", "item_id")
        .withColumn("ui_user_item_exploration_score", F.lit(0.3))
    )

    # session context
    w = Window.partitionBy("session_id").orderBy("ts")
    sess_pos = (
        base.withColumn("pos", F.row_number().over(w))
        .groupBy("user_id", "item_id")
        .agg(
            F.max("pos").alias("ui_position_in_session"),
            F.count("*").alias("ui_session_event_count"),
        )
    )

    # join all pieces
    df = (
        price_align.join(cat_match, ["user_id", "item_id"], "left")
        .join(cooc, ["user_id", "item_id"], "left")
        .join(rec, ["user_id", "item_id"], "left")
        .join(sim, ["user_id", "item_id"], "left")
        .join(expl, ["user_id", "item_id"], "left")
        .join(sess_pos, ["user_id", "item_id"], "left")
    )

    # recency bucket
    df = df.withColumn(
        "ui_recency_bucket",
        F.when(
            F.col("ui_time_since_user_last_interaction_with_item_sec") < 3600,
            0,
        )
        .when(
            F.col("ui_time_since_user_last_interaction_with_item_sec") < 6 * 3600,
            1,
        )
        .when(
            F.col("ui_time_since_user_last_interaction_with_item_sec") < 24 * 3600,
            2,
        )
        .otherwise(3),
    )

    return df


def main() -> None:
    paths = get_batch_paths()
    spark = get_spark_session("gold-cross-features")

    logger.info("Starting Gold cross features job.")
    ev, uf, itf = _read_gold_and_silver(
        paths.silver_events,
        paths.gold_user_features,
        paths.gold_item_features,
        spark,
    )

    feats = _build_cross_features(ev, uf, itf)
    partition_date = date.today().isoformat()

    (
        feats.withColumn("date", F.lit(partition_date))
        .write.mode("overwrite")
        .partitionBy("date")
        .parquet(paths.gold_cross_features)
    )

    logger.info("Gold cross features job done.")
    spark.stop()


if __name__ == "__main__":
    main()
