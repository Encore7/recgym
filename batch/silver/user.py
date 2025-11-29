"""
Silver layer job: derive silver_users from silver_events.

Responsibilities:
- Read silver_events.
- Aggregate per-user activity window:
  - first_seen_ts
  - last_seen_ts
  - total_events_30d (approx, for demo)
- Write Parquet to Silver/users.
"""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame, functions as F

from batch.config import get_batch_paths
from batch.utils import get_spark_session

logger = logging.getLogger(__name__)


def _build_silver_users(events: DataFrame) -> DataFrame:
    """
    Build silver_users table from event-level data.

    Output schema:
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


def main() -> None:
    """
    Entrypoint: run Silver users job.
    """
    paths = get_batch_paths()
    spark = get_spark_session("silver-users")

    logger.info("Reading Silver events.", extra={"path": paths.silver_events})
    events_df = spark.read.parquet(paths.silver_events)

    users_df = _build_silver_users(events_df)

    logger.info(
        "Writing Silver users.",
        extra={"output_path": paths.silver_users},
    )
    users_df.write.mode("overwrite").parquet(paths.silver_users)

    logger.info("Silver users job completed.")
    spark.stop()


if __name__ == "__main__":
    main()
