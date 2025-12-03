"""
Gold layer: item features from silver_events + silver_items.

Implements item features from feature_catalog:
- 30d volume / popularity / CTR
- monetary stats, distinct users
- recency (all-time)
- category / brand / discount / text stats
- temporal view shares (morning/evening)
- item_behavior_emb_1..10 via TF-IDF + PCA on category + text
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Tuple

from pyspark.ml import Pipeline
from pyspark.ml.feature import HashingTF, IDF, PCA, Tokenizer
from pyspark.sql import DataFrame, SparkSession, functions as F, types as T

from apps.batch.config import get_batch_paths
from apps.batch.utils.spark import get_spark_session
from libs.observability.instrumentation import init_observability
from libs.observability.tracing import get_tracer

logger = logging.getLogger(__name__)


def _compute_time_window(events: DataFrame) -> Tuple[datetime, datetime]:
    """
    Compute (max_ts, cutoff_30d) from silver_events.
    """
    row = events.agg(F.max("ts").alias("max_ts")).collect()[0]
    max_ts = row["max_ts"]
    if max_ts is None:
        raise RuntimeError("No events found in silver_events; cannot compute window.")

    if not isinstance(max_ts, datetime):
        raise RuntimeError(f"Expected timestamp for ts, got {type(max_ts)}")

    cutoff_30d = max_ts - timedelta(days=30)

    logger.info(
        "Computed item 30d window.",
        extra={
            "max_ts": max_ts.isoformat(),
            "cutoff_30d": cutoff_30d.isoformat(),
        },
    )
    return max_ts, cutoff_30d


def _split_events_by_window(
    events: DataFrame,
    cutoff_30d: datetime,
) -> DataFrame:
    """
    Filter events to 30d window.
    """
    return events.where(F.col("ts") >= F.lit(cutoff_30d))


def _volume_and_popularity(ev_30d: DataFrame) -> DataFrame:
    """
    Volume/popularity metrics over 30d.

    Outputs:
      item_views_30d
      item_carts_30d
      item_purchases_30d
      total_events_30d
      item_ctr_30d
      item_cart_to_purchase_rate_30d
      item_log_popularity_score_30d
    """
    agg = (
        ev_30d.groupBy("item_id")
        .agg(
            F.sum(F.when(F.col("event_type") == "view", 1).otherwise(0)).alias(
                "item_views_30d",
            ),
            F.sum(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)).alias(
                "item_carts_30d",
            ),
            F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias(
                "item_purchases_30d",
            ),
            F.count("*").alias("total_events_30d"),
        )
        .dropna(subset=["item_id"])
    )

    agg = (
        agg.withColumn(
            "item_ctr_30d",
            F.when(
                F.col("item_views_30d") > 0,
                F.col("item_purchases_30d") / F.col("item_views_30d"),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "item_cart_to_purchase_rate_30d",
            F.when(
                F.col("item_carts_30d") > 0,
                F.col("item_purchases_30d") / F.col("item_carts_30d"),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "item_log_popularity_score_30d",
            F.log(
                F.lit(1)
                + F.col("item_views_30d")
                + F.col("item_carts_30d")
                + F.col("item_purchases_30d"),
            ),
        )
    )

    return agg


def _monetary_and_users(ev_30d: DataFrame) -> DataFrame:
    """
    Monetary stats + distinct users from 30d window.

    Outputs:
      item_avg_price_30d
      item_price_std_30d
      item_max_price_30d
      item_min_price_30d
      item_distinct_users_viewed_30d
      item_distinct_users_purchased_30d
    """
    monetary = (
        ev_30d.groupBy("item_id")
        .agg(
            F.avg("price").alias("item_avg_price_30d"),
            F.stddev("price").alias("item_price_std_30d"),
            F.max("price").alias("item_max_price_30d"),
            F.min("price").alias("item_min_price_30d"),
        )
        .dropna(subset=["item_id"])
    )

    disc = (
        ev_30d.groupBy("item_id")
        .agg(
            F.countDistinct(
                F.when(F.col("event_type") == "view", F.col("user_id")),
            ).alias("item_distinct_users_viewed_30d"),
            F.countDistinct(
                F.when(F.col("event_type") == "purchase", F.col("user_id")),
            ).alias("item_distinct_users_purchased_30d"),
        )
    )

    return monetary.join(disc, "item_id", "left")


def _recency_all(ev_all: DataFrame) -> DataFrame:
    """
    Recency metrics using ALL history.

    Outputs:
      item_time_since_last_event_sec
      item_time_since_last_view_sec
      item_time_since_last_purchase_sec
    """
    rec = (
        ev_all.groupBy("item_id")
        .agg(
            F.max("ts").alias("last_ts"),
            F.max(F.when(F.col("event_type") == "view", F.col("ts"))).alias(
                "last_view_ts",
            ),
            F.max(F.when(F.col("event_type") == "purchase", F.col("ts"))).alias(
                "last_purchase_ts",
            ),
        )
        .dropna(subset=["item_id"])
    )

    now = F.current_timestamp()

    return (
        rec.withColumn(
            "item_time_since_last_event_sec",
            F.unix_timestamp(now) - F.unix_timestamp("last_ts"),
        )
        .withColumn(
            "item_time_since_last_view_sec",
            F.unix_timestamp(now) - F.unix_timestamp("last_view_ts"),
        )
        .withColumn(
            "item_time_since_last_purchase_sec",
            F.unix_timestamp(now) - F.unix_timestamp("last_purchase_ts"),
        )
    )


def _category_brand_text(items: DataFrame, ev_30d: DataFrame) -> DataFrame:
    """
    Category/brand/text features from silver_items (+discount logic).
    """
    base = (
        items.select(
            "item_id",
            "category_path",
            "brand",
            "base_price",
            "title",
            "description",
        )
        .dropna(subset=["item_id"])
        .withColumn("item_category_depth", F.size("category_path"))
        # Simple discount flag:
        # For now: 1 if base_price > 0 (simulating discount presence)
        .withColumn(
            "item_is_discounted_flag",
            F.when(F.col("base_price") > 0.0, F.lit(1)).otherwise(F.lit(0)),
        )
        .withColumn(
            "item_title_length_chars",
            F.length(F.col("title")),
        )
        .withColumn(
            "item_description_length_chars",
            F.length(F.col("description")),
        )
        # For demo, static counts for num_images / num_attributes
        .withColumn("item_num_images", F.lit(3))
        .withColumn("item_num_attributes", F.lit(8))
    )

    return base


def _temporal_shares(ev_30d: DataFrame) -> DataFrame:
    """
    Temporal shares per item (30d).
    """
    ev = ev_30d.withColumn("hour", F.hour("ts"))

    tmp = (
        ev.groupBy("item_id")
        .agg(
            F.sum(F.when(F.col("hour").between(6, 11), 1).otherwise(0)).alias(
                "morning",
            ),
            F.sum(F.when(F.col("hour").between(18, 23), 1).otherwise(0)).alias(
                "evening",
            ),
            F.count("*").alias("total"),
        )
        .dropna(subset=["item_id"])
    )

    return (
        tmp.withColumn(
            "item_morning_view_share_30d",
            F.col("morning") / F.col("total"),
        )
        .withColumn(
            "item_evening_view_share_30d",
            F.col("item_evening_view_share_30d"),
        )
        .withColumn(
            "item_evening_view_share_30d",
            F.col("evening") / F.col("total"),
        )
        .select(
            "item_id",
            "item_morning_view_share_30d",
            "item_evening_view_share_30d",
        )
    )


def _item_behavior_embeddings(
    items: DataFrame,
    spark: SparkSession,
) -> DataFrame:
    """
    Build item_behavior_emb_1..10 via TF-IDF + PCA on:

      text = category_path + brand + title
    """
    # Prepare text field from category_path + brand + title
    text_src = (
        items.select("item_id", "category_path", "brand", "title")
        .dropna(subset=["item_id"])
        .withColumn("cats", F.concat_ws(" ", "category_path"))
        .withColumn(
            "text",
            F.concat_ws(
                " ",
                "cats",
                "brand",
                "title",
            ),
        )
        .select("item_id", "text")
        .where(F.col("text").isNotNull() & (F.length("text") > 0))
    )

    if text_src.rdd.isEmpty():
        schema = T.StructType(
            [T.StructField("item_id", T.StringType(), nullable=False)]
            + [
                T.StructField(f"item_behavior_emb_{i}", T.FloatType(), nullable=True)
                for i in range(1, 11)
            ],
        )
        return spark.createDataFrame([], schema=schema)

    tokenizer = Tokenizer(inputCol="text", outputCol="tokens")
    hashing_tf = HashingTF(
        inputCol="tokens",
        outputCol="raw_features",
        numFeatures=512,
    )
    idf = IDF(inputCol="raw_features", outputCol="tfidf_features")
    pca = PCA(k=10, inputCol="tfidf_features", outputCol="pca_features")

    pipeline = Pipeline(stages=[tokenizer, hashing_tf, idf, pca])
    model = pipeline.fit(text_src)
    transformed = model.transform(text_src).select("item_id", "pca_features")

    emb_df = transformed
    for i in range(10):
        emb_df = emb_df.withColumn(
            f"item_behavior_emb_{i + 1}",
            F.col("pca_features")[i].cast("float"),
        )

    return emb_df.select(
        "item_id",
        *[f"item_behavior_emb_{i}" for i in range(1, 11)],
    )


def _build_item_features(
    events: DataFrame,
    items: DataFrame,
    spark: SparkSession,
) -> DataFrame:
    """
    Compose all item features into one DF.
    """
    max_ts, cutoff_30d = _compute_time_window(events)
    ev_30d = _split_events_by_window(events, cutoff_30d)

    vol = _volume_and_popularity(ev_30d)
    monet_users = _monetary_and_users(ev_30d)
    rec = _recency_all(events)
    cat_brand_text = _category_brand_text(items, ev_30d)
    tmp = _temporal_shares(ev_30d)
    emb = _item_behavior_embeddings(items, spark)

    df = (
        vol.join(monet_users, "item_id", "left")
        .join(rec, "item_id", "left")
        .join(cat_brand_text, "item_id", "left")
        .join(tmp, "item_id", "left")
        .join(emb, "item_id", "left")
    )

    return df


def main() -> None:
    """
    Gold item features batch job.

    Reads:
      - Silver events (Delta)
      - Silver items (Delta)
    Writes:
      - Gold item features (Delta, partitioned by date)
    """
    init_observability()
    tracer = get_tracer("recgym.batch.gold.item_features")

    with tracer.start_as_current_span("gold_item_features_main"):
        paths = get_batch_paths()
        spark = get_spark_session("gold-item-features")

        logger.info(
            "Starting Gold item features job.",
            extra={
                "silver_events_path": paths.silver_events,
                "silver_items_path": paths.silver_items,
            },
        )

        events = spark.read.format("delta").load(paths.silver_events)
        items = spark.read.format("delta").load(paths.silver_items)

        feats = _build_item_features(events, items, spark)

        partition_date = datetime.utcnow().date().isoformat()

        (
            feats.withColumn("date", F.lit(partition_date))
            .write.format("delta")
            .mode("overwrite")
            .partitionBy("date")
            .save(paths.gold_item_features)
        )

        logger.info(
            "Gold item features job completed.",
            extra={"output_path": paths.gold_item_features, "date": partition_date},
        )
        spark.stop()


if __name__ == "__main__":
    main()
