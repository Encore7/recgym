"""
Silver layer job: bronze → silver_events.

Responsibilities:
- Read raw events from Bronze (Kafka Connect → MinIO).
- Normalize schema to canonical event schema.
- Convert ts (millis) → timestamp.
- Basic cleaning: drop bad rows, ensure price > 0.
- Write partitioned Parquet to Silver/events.
"""

from __future__ import annotations

import logging
from typing import List

from pyspark.sql import DataFrame, functions as F, types as T

from batch.config import get_batch_paths
from batch.utils import get_spark_session

logger = logging.getLogger(__name__)


def _silver_schema() -> T.StructType:
    """
    Canonical Silver schema for events.

    This should match libs.models.events.RetailEvent, but with ts as timestamp.
    """
    return T.StructType(
        [
            T.StructField("user_id", T.StringType(), nullable=False),
            T.StructField("session_id", T.StringType(), nullable=False),
            T.StructField("item_id", T.StringType(), nullable=False),
            T.StructField("event_type", T.StringType(), nullable=False),
            T.StructField("price", T.DoubleType(), nullable=False),
            T.StructField("category_path", T.ArrayType(T.StringType()), nullable=True),
            T.StructField("device", T.StringType(), nullable=False),
            T.StructField("page", T.StringType(), nullable=False),
            T.StructField("referrer", T.StringType(), nullable=True),
            T.StructField("ts", T.TimestampType(), nullable=False),
        ]
    )


def _normalize_ts(df: DataFrame) -> DataFrame:
    """
    Normalize ts column to timestamp (UTC).

    If ts is long/bigint in millis, convert → timestamp.
    """
    dtypes = dict(df.dtypes)
    if "ts" not in dtypes:
        raise RuntimeError("Bronze events missing 'ts' column.")

    if dtypes["ts"] in ("bigint", "long", "int"):
        df = df.withColumn("ts", (F.col("ts") / F.lit(1000.0)).cast("timestamp"))

    return df


def _to_silver(df: DataFrame) -> DataFrame:
    """
    Transform Bronze → Silver events.

    - enforce columns
    - cast to canonical types
    - basic filters
    - add event_date / event_hour partitions
    """
    schema = _silver_schema()
    required_cols: List[str] = [field.name for field in schema]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise RuntimeError(f"Bronze events missing required columns: {missing}")

    df = _normalize_ts(df)

    silver = df.select(
        F.col("user_id").cast(T.StringType()),
        F.col("session_id").cast(T.StringType()),
        F.col("item_id").cast(T.StringType()),
        F.col("event_type").cast(T.StringType()),
        F.col("price").cast(T.DoubleType()),
        F.col("category_path").cast(T.ArrayType(T.StringType())),
        F.col("device").cast(T.StringType()),
        F.col("page").cast(T.StringType()),
        F.col("referrer").cast(T.StringType()),
        F.col("ts").cast(T.TimestampType()),
    )

    silver = silver.dropna(
        subset=["user_id", "session_id", "item_id", "event_type", "ts"],
    ).filter(F.col("price") > 0.0)

    silver = silver.withColumn("event_date", F.to_date("ts")).withColumn(
        "event_hour", F.hour("ts")
    )

    silver = silver.dropDuplicates(
    ["user_id", "session_id", "item_id", "event_type", "ts"]
)

    return silver


def main() -> None:
    """
    Entrypoint: run Bronze → Silver events job.
    """
    paths = get_batch_paths()
    spark = get_spark_session("silver-events")

    logger.info("Reading Bronze events.", extra={"path": paths.bronze_raw})
    bronze_df = bronze_df.filter(F.col("user_id").isNotNull())
    bronze_df = (
        spark.read.option("recursiveFileLookup", "true").parquet(paths.bronze_raw)
    )

    silver_df = _to_silver(bronze_df)

    logger.info(
        "Writing Silver events.",
        extra={"output_path": paths.silver_events},
    )
    (
        silver_df.repartition("event_date", "event_hour")
        .write.mode("overwrite")
        .partitionBy("event_date", "event_hour")
        .parquet(paths.silver_events)
    )

    logger.info("Silver events job completed.")
    spark.stop()


if __name__ == "__main__":
    main()
