"""
Gold layer: user-item cross features.

Joins:
  - Gold user features (gold_user_features)
  - Gold item features (gold_item_features)
  - Silver events (silver_events)  for co-occurrence + recency + session context

Implements key cross features from feature_catalog:
  - ui_price_diff_from_user_avg
  - ui_price_zscore_for_user
  - ui_same_category_flag
  - ui_same_top_category_flag
  - ui_user_item_co_views_30d / carts / purchases
  - ui_time_since_user_last_interaction_with_item_sec
  - ui_position_in_session
  - ui_session_event_count
  - ui_recency_bucket
  - ui_user_item_similarity_emb (dot product of embeddings)
  - ui_user_item_exploration_score (simple heuristic: 1 / (1 + co_views_30d))
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from pyspark.sql import DataFrame, Window, functions as F

from apps.batch.config import get_batch_paths
from apps.batch.utils.spark import get_spark_session
from libs.observability.instrumentation import init_observability
from libs.observability.tracing import get_tracer

logger = logging.getLogger(__name__)


def _compute_time_window(events: DataFrame) -> datetime:
    """
    Compute cutoff_30d for cross co-occurrence metrics.
    """
    row = events.agg(F.max("ts").alias("max_ts")).collect()[0]
    max_ts = row["max_ts"]
    if max_ts is None:
        raise RuntimeError("No events in silver_events; cannot compute cross window.")

    if not isinstance(max_ts, datetime):
        raise RuntimeError(f"Expected timestamp for ts, got {type(max_ts)}")

    cutoff_30d = max_ts - timedelta(days=30)

    logger.info(
        "Computed cross 30d window.",
        extra={"max_ts": max_ts.isoformat(), "cutoff_30d": cutoff_30d.isoformat()},
    )
    return cutoff_30d


def _base_pairs(events: DataFrame) -> DataFrame:
    """
    Base (user_id, item_id, ts, event_type, session_id) pairs for cross features.
    """
    return (
        events.select("user_id", "item_id", "ts", "event_type", "session_id")
        .dropna(subset=["user_id", "item_id"])
        .dropDuplicates(["user_id", "item_id", "ts"])
    )


def _recency_per_user_item(base: DataFrame) -> DataFrame:
    """
    Last interaction per (user, item) and recency.

    Produces:
      ui_time_since_user_last_interaction_with_item_sec
    """
    rec = (
        base.groupBy("user_id", "item_id")
        .agg(F.max("ts").alias("last_ui_ts"))
        .dropna(subset=["user_id", "item_id"])
    )

    now = F.current_timestamp()

    return rec.withColumn(
        "ui_time_since_user_last_interaction_with_item_sec",
        F.unix_timestamp(now) - F.unix_timestamp("last_ui_ts"),
    )


def _cooccurrence_30d(base_30d: DataFrame) -> DataFrame:
    """
    Co-occurrence counts over 30d window.

    Produces:
      ui_user_item_co_views_30d
      ui_user_item_co_carts_30d
      ui_user_item_co_purchases_30d
    """
    return (
        base_30d.groupBy("user_id", "item_id")
        .agg(
            F.sum(F.when(F.col("event_type") == "view", 1).otherwise(0)).alias(
                "ui_user_item_co_views_30d",
            ),
            F.sum(F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)).alias(
                "ui_user_item_co_carts_30d",
            ),
            F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias(
                "ui_user_item_co_purchases_30d",
            ),
        )
        .dropna(subset=["user_id", "item_id"])
    )


def _session_context(base: DataFrame) -> DataFrame:
    """
    Session context features:

      ui_position_in_session
      ui_session_event_count
    """
    w_sess = Window.partitionBy("session_id").orderBy("ts")

    with_pos = base.withColumn("pos", F.row_number().over(w_sess))

    sess_ctx = (
        with_pos.groupBy("user_id", "item_id", "session_id")
        .agg(
            F.max("pos").alias("max_pos"),
            F.count("*").alias("session_events"),
        )
        .dropna(subset=["user_id", "item_id"])
    )

    # For each (user, item), take latest session
    w_ui = Window.partitionBy("user_id", "item_id").orderBy(F.desc("session_events"))

    latest = (
        sess_ctx.withColumn("rn", F.row_number().over(w_ui))
        .where(F.col("rn") == 1)
        .select(
            "user_id",
            "item_id",
            F.col("max_pos").alias("ui_position_in_session"),
            F.col("session_events").alias("ui_session_event_count"),
        )
    )

    return latest


def _category_matches(
    user_feats: DataFrame,
    item_meta: DataFrame,
) -> DataFrame:
    """
    Category match features using:

      user_top_category_1/2/3 (user features)
      category_path (item)
    """
    joined = (
        item_meta.select("item_id", "category_path")
        .join(user_feats.select("user_id", "user_top_category_1"), how="inner")
        .dropna(subset=["user_id", "item_id"])
    )

    # We'll join by user_id & item_id later, so only keep per pair output
    cat_match = (
        joined.withColumn(
            "ui_same_category_flag",
            F.when(
                F.array_contains(
                    F.col("category_path"),
                    F.col("user_top_category_1"),
                ),
                F.lit(1),
            ).otherwise(F.lit(0)),
        )
        .withColumn(
            "ui_same_top_category_flag",
            F.when(
                F.col("user_top_category_1")
                == F.element_at("category_path", 1),
                1,
            ).otherwise(0),
        )
        .select(
            "user_id",
            "item_id",
            "ui_same_category_flag",
            "ui_same_top_category_flag",
        )
    )

    return cat_match


def _price_alignment(
    user_feats: DataFrame,
    item_feats: DataFrame,
) -> DataFrame:
    """
    Price alignment features using:

      user_avg_price_30d, user_price_std_30d
      item_avg_price_30d
    """
    joined = (
        user_feats.select("user_id", "user_avg_price_30d", "user_price_std_30d")
        .join(
            item_feats.select("item_id", "item_avg_price_30d"),
            how="inner",
        )
    )

    return joined.select(
        "user_id",
        "item_id",
        (F.col("item_avg_price_30d") - F.col("user_avg_price_30d")).alias(
            "ui_price_diff_from_user_avg",
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


def _embedding_similarity(
    user_feats: DataFrame,
    item_feats: DataFrame,
) -> DataFrame:
    """
    Similarity + exploration score using user/item embeddings:

      ui_user_item_similarity_emb
      ui_user_item_exploration_score

    Heuristic:
      - similarity = dot(user_emb, item_emb)
      - exploration_score = 1 / (1 + log(1 + co_views_30d))
        (filled later when co-occurrence joined)
    """
    # Only keep IDs + embedding columns
    u_cols = ["user_id"] + [f"user_behavior_emb_{i}" for i in range(1, 11)]
    i_cols = ["item_id"] + [f"item_behavior_emb_{i}" for i in range(1, 11)]

    uf = user_feats.select(*u_cols)
    itf = item_feats.select(*i_cols)

    joined = uf.crossJoin(itf)  # NOTE: can be restricted later; for demo only.

    similarity_expr = None
    for i in range(1, 11):
        term = (
            F.col(f"user_behavior_emb_{i}") * F.col(f"item_behavior_emb_{i}")
        )
        similarity_expr = term if similarity_expr is None else similarity_expr + term

    return joined.select(
        "user_id",
        "item_id",
        similarity_expr.alias("ui_user_item_similarity_emb"),
    )


def _recency_bucket(df: DataFrame) -> DataFrame:
    """
    Bucketize recency (0–3) based on ui_time_since_user_last_interaction_with_item_sec.
    """
    return df.withColumn(
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


def _build_cross_features(
    events: DataFrame,
    user_feats: DataFrame,
    item_feats: DataFrame,
) -> DataFrame:
    """
    Compose all cross features into one (user_id, item_id)-grain DF.
    """
    cutoff_30d = _compute_time_window(events)

    base_all = _base_pairs(events)
    base_30d = base_all.where(F.col("ts") >= F.lit(cutoff_30d))

    rec = _recency_per_user_item(base_all)
    cooc = _cooccurrence_30d(base_30d)
    sess = _session_context(base_all)

    # Category & price alignment
    cat_match = _category_matches(user_feats, item_feats)
    price_align = _price_alignment(user_feats, item_feats)

    # Embedding similarity
    emb_sim = _embedding_similarity(user_feats, item_feats)

    # Start joining on (user_id, item_id)
    df = (
        price_align.join(cat_match, ["user_id", "item_id"], "left")
        .join(cooc, ["user_id", "item_id"], "left")
        .join(rec, ["user_id", "item_id"], "left")
        .join(sess, ["user_id", "item_id"], "left")
        .join(emb_sim, ["user_id", "item_id"], "left")
    )

    # Exploration score heuristic: smaller co_views_30d ⇒ higher exploration
    df = df.withColumn(
        "ui_user_item_exploration_score",
        F.when(
            F.col("ui_user_item_co_views_30d").isNotNull(),
            1.0
            / (
                1.0
                + F.log1p(
                    F.col("ui_user_item_co_views_30d").cast("double"),
                )
            ),
        ).otherwise(F.lit(1.0)),
    )

    df = _recency_bucket(df)

    return df


def main() -> None:
    """
    Gold cross (user-item) features batch job.

    Reads:
      - Silver events (Delta)
      - Gold user features (Delta)
      - Gold item features (Delta)
    Writes:
      - Gold user_item features (Delta, partitioned by date)
    """
    init_observability()
    tracer = get_tracer("recgym.batch.gold.cross_features")

    with tracer.start_as_current_span("gold_cross_features_main"):
        paths = get_batch_paths()
        spark = get_spark_session("gold-cross-features")

        logger.info(
            "Starting Gold cross features job.",
            extra={
                "silver_events_path": paths.silver_events,
                "gold_user_features_path": paths.gold_user_features,
                "gold_item_features_path": paths.gold_item_features,
            },
        )

        events = spark.read.format("delta").load(paths.silver_events)
        user_feats = spark.read.format("delta").load(paths.gold_user_features)
        item_feats = spark.read.format("delta").load(paths.gold_item_features)

        feats = _build_cross_features(events, user_feats, item_feats)

        partition_date = datetime.utcnow().date().isoformat()

        (
            feats.withColumn("date", F.lit(partition_date))
            .write.format("delta")
            .mode("overwrite")
            .partitionBy("date")
            .save(paths.gold_cross_features)
        )

        logger.info(
            "Gold cross features job done.",
            extra={"output_path": paths.gold_cross_features, "date": partition_date},
        )
        spark.stop()


if __name__ == "__main__":
    main()
