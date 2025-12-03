"""
Gold layer: user features from silver_events.

Implements ~40 user features as per feature_catalog:
- 30d volume / diversity / monetary stats
- recency & lifetime metrics (all-time)
- session stats (30d + last session)
- device & temporal mix (30d)
- category preferences (30d)
- user_behavior_emb_1..10 via TF-IDF + PCA on category_path

Windows:
- 30d window is based on max(ts) in silver_events (event-time, not wall-clock).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Tuple

from pyspark.ml import Pipeline
from pyspark.ml.feature import HashingTF, IDF, PCA, Tokenizer
from pyspark.sql import DataFrame, SparkSession, Window, functions as F, types as T

from apps.batch.config import get_batch_paths
from apps.batch.utils.spark import get_spark_session
from libs.observability.instrumentation import init_observability
from libs.observability.tracing import get_tracer

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class TimeWindows:
    """Event-time windows anchored at max(ts)."""

    max_ts: datetime
    cutoff_30d: datetime
    cutoff_7d: datetime


def _compute_time_windows(events: DataFrame) -> TimeWindows:
    """
    Compute event-time windows based on max(ts) in data.

    Uses:
      - max_ts = max(ts)
      - cutoff_30d = max_ts - 30 days
      - cutoff_7d  = max_ts - 7 days
    """
    row = events.agg(F.max("ts").alias("max_ts")).collect()[0]
    max_ts = row["max_ts"]
    if max_ts is None:
        raise RuntimeError("No events found in silver_events; cannot compute windows.")

    if not isinstance(max_ts, datetime):
        raise RuntimeError(f"Expected timestamp for ts, got {type(max_ts)}")

    cutoff_30d = max_ts - timedelta(days=30)
    cutoff_7d = max_ts - timedelta(days=7)

    logger.info(
        "Computed event-time windows.",
        extra={
            "max_ts": max_ts.isoformat(),
            "cutoff_30d": cutoff_30d.isoformat(),
            "cutoff_7d": cutoff_7d.isoformat(),
        },
    )

    return TimeWindows(max_ts=max_ts, cutoff_30d=cutoff_30d, cutoff_7d=cutoff_7d)


def _split_events_by_window(
    events: DataFrame,
    tw: TimeWindows,
) -> Tuple[DataFrame, DataFrame, DataFrame]:
    """
    Split into full / 30d / 7d windows (event-time).
    """
    ev_all = events
    ev_30d = events.where(F.col("ts") >= F.lit(tw.cutoff_30d))
    ev_7d = events.where(F.col("ts") >= F.lit(tw.cutoff_7d))
    return ev_all, ev_30d, ev_7d


def _volume_and_diversity(ev_30d: DataFrame) -> DataFrame:
    """
    Volume + diversity metrics over 30d window.

    Produces:
      user_total_events_30d
      user_views_30d
      user_add_to_cart_30d
      user_purchases_30d
      user_distinct_items_viewed_30d
      user_distinct_categories_viewed_30d
    """
    return (
        ev_30d.groupBy("user_id")
        .agg(
            F.count("*").alias("user_total_events_30d"),
            F.sum(F.when(F.col("event_type") == "view", 1).otherwise(0)).alias(
                "user_views_30d",
            ),
            F.sum(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)).alias(
                "user_add_to_cart_30d",
            ),
            F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias(
                "user_purchases_30d",
            ),
            F.countDistinct("item_id").alias("user_distinct_items_viewed_30d"),
            F.countDistinct(F.explode_outer("category_path")).alias(
                "user_distinct_categories_viewed_30d",
            ),
        )
        .dropna(subset=["user_id"])
    )


def _conversion_rates(base: DataFrame) -> DataFrame:
    """
    Conversion features derived from base volume metrics.
    """
    return (
        base.withColumn(
            "user_view_to_cart_rate_30d",
            F.when(
                F.col("user_views_30d") > 0,
                F.col("user_add_to_cart_30d") / F.col("user_views_30d"),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "user_cart_to_purchase_rate_30d",
            F.when(
                F.col("user_add_to_cart_30d") > 0,
                F.col("user_purchases_30d") / F.col("user_add_to_cart_30d"),
            ).otherwise(F.lit(0.0)),
        )
    )


def _monetary_stats(ev_30d: DataFrame) -> DataFrame:
    """
    Monetary stats over 30d window.

    Produces:
      user_avg_price_30d
      user_price_std_30d
      user_max_price_30d
      user_min_price_30d
    """
    return (
        ev_30d.groupBy("user_id")
        .agg(
            F.avg("price").alias("user_avg_price_30d"),
            F.stddev("price").alias("user_price_std_30d"),
            F.max("price").alias("user_max_price_30d"),
            F.min("price").alias("user_min_price_30d"),
        )
        .dropna(subset=["user_id"])
    )


def _recency_and_lifetime(ev_all: DataFrame) -> DataFrame:
    """
    Recency + lifetime features using ALL history.

    Produces:
      user_time_since_last_event_sec
      user_time_since_last_view_sec
      user_time_since_last_cart_sec
      user_time_since_last_purchase_sec
      user_days_since_first_event
    """
    rec = (
        ev_all.groupBy("user_id")
        .agg(
            F.max("ts").alias("last_ts"),
            F.min("ts").alias("first_ts"),
            F.max(F.when(F.col("event_type") == "view", F.col("ts"))).alias(
                "last_view_ts",
            ),
            F.max(F.when(F.col("event_type") == "add_to_cart", F.col("ts"))).alias(
                "last_cart_ts",
            ),
            F.max(F.when(F.col("event_type") == "purchase", F.col("ts"))).alias(
                "last_purchase_ts",
            ),
        )
        .dropna(subset=["user_id"])
    )

    now = F.current_timestamp()

    return (
        rec.withColumn(
            "user_time_since_last_event_sec",
            F.unix_timestamp(now) - F.unix_timestamp("last_ts"),
        )
        .withColumn(
            "user_time_since_last_view_sec",
            F.unix_timestamp(now) - F.unix_timestamp("last_view_ts"),
        )
        .withColumn(
            "user_time_since_last_cart_sec",
            F.unix_timestamp(now) - F.unix_timestamp("last_cart_ts"),
        )
        .withColumn(
            "user_time_since_last_purchase_sec",
            F.unix_timestamp(now) - F.unix_timestamp("last_purchase_ts"),
        )
        .withColumn(
            "user_days_since_first_event",
            (F.unix_timestamp(now) - F.unix_timestamp("first_ts")) / F.lit(86400.0),
        )
    )


def _session_metrics(ev_30d: DataFrame, ev_all: DataFrame) -> DataFrame:
    """
    Session-level metrics:

      user_avg_session_length_events_30d
      user_median_session_length_events_30d
      user_sessions_7d   → will be injected separately
      user_events_last_session
    """
    # Session length in 30d window
    sess_30d = (
        ev_30d.groupBy("user_id", "session_id")
        .agg(F.count("*").alias("session_len"))
        .groupBy("user_id")
        .agg(
            F.avg("session_len").alias("user_avg_session_length_events_30d"),
            F.expr("percentile(session_len, 0.5)").alias(
                "user_median_session_length_events_30d",
            ),
        )
    )

    # Most recent session from ALL history
    sess_all = (
        ev_all.groupBy("user_id", "session_id")
        .agg(
            F.count("*").alias("session_events"),
            F.max("ts").alias("session_last_ts"),
        )
    )

    w_last = Window.partitionBy("user_id").orderBy(F.desc("session_last_ts"))
    last_session = (
        sess_all.withColumn("rn", F.row_number().over(w_last))
        .where(F.col("rn") == 1)
        .select(
            "user_id",
            F.col("session_events").alias("user_events_last_session"),
        )
    )

    return sess_30d.join(last_session, "user_id", "left")


def _sessions_7d(ev_7d: DataFrame) -> DataFrame:
    """
    Sessions in the last 7 days (event-time).

    Produces:
      user_sessions_7d
    """
    return (
        ev_7d.groupBy("user_id", "session_id")
        .agg(F.count("*").alias("session_len"))
        .groupBy("user_id")
        .agg(F.count("*").alias("user_sessions_7d"))
    )


def _device_mix(ev_30d: DataFrame) -> DataFrame:
    """
    Device mix over 30d window.

    Produces:
      user_web_events_share_30d
      user_ios_events_share_30d
      user_android_events_share_30d
    """
    dev = (
        ev_30d.groupBy("user_id")
        .agg(
            F.sum(F.when(F.col("device") == "web", 1).otherwise(0)).alias("web"),
            F.sum(F.when(F.col("device") == "ios", 1).otherwise(0)).alias("ios"),
            F.sum(F.when(F.col("device") == "android", 1).otherwise(0)).alias(
                "android",
            ),
            F.count("*").alias("total"),
        )
    )

    return (
        dev.withColumn(
            "user_web_events_share_30d",
            F.col("web") / F.col("total"),
        )
        .withColumn(
            "user_ios_events_share_30d",
            F.col("user_ios_events_share_30d"),
        )
        .withColumn(
            "user_ios_events_share_30d",
            F.col("ios") / F.col("total"),
        )
        .withColumn(
            "user_android_events_share_30d",
            F.col("android") / F.col("total"),
        )
        .select(
            "user_id",
            "user_web_events_share_30d",
            "user_ios_events_share_30d",
            "user_android_events_share_30d",
        )
    )


def _temporal_mix(ev_30d: DataFrame) -> DataFrame:
    """
    Temporal distribution (30d) – morning/evening/night shares.
    """
    ev = ev_30d.withColumn("hour", F.hour("ts"))

    tmp = (
        ev.groupBy("user_id")
        .agg(
            F.sum(F.when(F.col("hour").between(6, 11), 1).otherwise(0)).alias(
                "morning",
            ),
            F.sum(F.when(F.col("hour").between(18, 23), 1).otherwise(0)).alias(
                "evening",
            ),
            F.sum(F.when(F.col("hour").between(0, 5), 1).otherwise(0)).alias("night"),
            F.count("*").alias("total"),
        )
    )

    return (
        tmp.withColumn(
            "user_morning_event_share_30d",
            F.col("morning") / F.col("total"),
        )
        .withColumn(
            "user_evening_event_share_30d",
            F.col("evening") / F.col("total"),
        )
        .withColumn(
            "user_night_event_share_30d",
            F.col("night") / F.col("total"),
        )
        .select(
            "user_id",
            "user_morning_event_share_30d",
            "user_evening_event_share_30d",
            "user_night_event_share_30d",
        )
    )


def _top_categories(ev_30d: DataFrame) -> DataFrame:
    """
    Top-3 categories over 30d, based on freq of categories in category_path.
    """
    cat = (
        ev_30d.withColumn("cat", F.explode("category_path"))
        .groupBy("user_id", "cat")
        .count()
        .withColumn(
            "rn",
            F.row_number().over(
                Window.partitionBy("user_id").orderBy(F.desc("count")),
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

    return top3


def _user_behavior_embeddings(ev_30d: DataFrame, spark: SparkSession) -> DataFrame:
    """
    Build 10-d user_behavior_emb_* via TF-IDF + PCA on category sequences.

    Steps:
      - explode category_path → per (user, category)
      - collect_list(category) per user → "cats"
      - join categories into a text string
      - TF-IDF over tokens
      - PCA(k=10) over TF-IDF vector
    """
    cats = (
        ev_30d.select("user_id", F.explode("category_path").alias("cat"))
        .dropna(subset=["user_id", "cat"])
        .groupBy("user_id")
        .agg(F.collect_list("cat").alias("cats"))
    )

    if cats.rdd.isEmpty():
        # No categories – return empty DF with correct schema
        schema = T.StructType(
            [T.StructField("user_id", T.StringType(), nullable=False)]
            + [
                T.StructField(f"user_behavior_emb_{i}", T.FloatType(), nullable=True)
                for i in range(1, 11)
            ],
        )
        return spark.createDataFrame([], schema=schema)

    text_df = cats.withColumn("text", F.concat_ws(" ", "cats")).select(
        "user_id",
        "text",
    )

    tokenizer = Tokenizer(inputCol="text", outputCol="tokens")
    hashing_tf = HashingTF(
        inputCol="tokens",
        outputCol="raw_features",
        numFeatures=512,
    )
    idf = IDF(inputCol="raw_features", outputCol="tfidf_features")
    pca = PCA(k=10, inputCol="tfidf_features", outputCol="pca_features")

    pipeline = Pipeline(stages=[tokenizer, hashing_tf, idf, pca])
    model = pipeline.fit(text_df)
    transformed = model.transform(text_df).select("user_id", "pca_features")

    emb_df = transformed
    for i in range(10):
        emb_df = emb_df.withColumn(
            f"user_behavior_emb_{i + 1}",
            F.col("pca_features")[i].cast("float"),
        )

    return emb_df.select(
        "user_id",
        *[f"user_behavior_emb_{i}" for i in range(1, 11)],
    )


def _build_user_features(events: DataFrame, spark: SparkSession) -> DataFrame:
    """
    Compose all user feature pieces into one wide DF.
    """
    tw = _compute_time_windows(events)
    ev_all, ev_30d, ev_7d = _split_events_by_window(events, tw)

    base = _volume_and_diversity(ev_30d)
    base = _conversion_rates(base)

    monetary = _monetary_stats(ev_30d)
    rec = _recency_and_lifetime(ev_all)
    sess = _session_metrics(ev_30d, ev_all)
    sess_7d = _sessions_7d(ev_7d)
    dev = _device_mix(ev_30d)
    tmp = _temporal_mix(ev_30d)
    cat = _top_categories(ev_30d)
    emb = _user_behavior_embeddings(ev_30d, spark)

    df = (
        base.join(monetary, "user_id", "left")
        .join(rec, "user_id", "left")
        .join(sess, "user_id", "left")
        .join(sess_7d, "user_id", "left")
        .join(dev, "user_id", "left")
        .join(tmp, "user_id", "left")
        .join(cat, "user_id", "left")
        .join(emb, "user_id", "left")
    )

    return df


def main() -> None:
    """
    Gold user features batch job.

    Reads:
      - Silver events (Delta)
    Writes:
      - Gold user features (Delta, partitioned by date)
    """
    init_observability()
    tracer = get_tracer("recgym.batch.gold.user_features")

    with tracer.start_as_current_span("gold_user_features_main"):
        paths = get_batch_paths()
        spark = get_spark_session("gold-user-features")

        logger.info(
            "Starting Gold user features job.",
            extra={"silver_events_path": paths.silver_events},
        )

        events = spark.read.format("delta").load(paths.silver_events)
        feats = _build_user_features(events, spark)

        partition_date = datetime.utcnow().date().isoformat()

        (
            feats.withColumn("date", F.lit(partition_date))
            .write.format("delta")
            .mode("overwrite")
            .partitionBy("date")
            .save(paths.gold_user_features)
        )

        logger.info(
            "Gold user features job complete.",
            extra={"output_path": paths.gold_user_features, "date": partition_date},
        )
        spark.stop()


if __name__ == "__main__":
    main()
