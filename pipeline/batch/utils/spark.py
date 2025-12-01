"""
Spark utilities for batch jobs.

Provides:
- get_spark_session: SparkSession configured for MinIO (S3A) and Parquet IO.
"""

from __future__ import annotations

import logging

from pyspark.sql import SparkSession

from libs.config import AppConfig

logger = logging.getLogger(__name__)


def get_spark_session(app_name: str) -> SparkSession:
    """
    Create a SparkSession configured for MinIO S3A access.

    Uses:
        - AppConfig.s3.endpoint_url
        - AppConfig.s3.access_key
        - AppConfig.s3.secret_key

    Args:
        app_name: Logical name of the Spark application.

    Returns:
        A configured SparkSession instance.
    """
    config = AppConfig.load()
    s3_cfg = config.s3

    logger.info(
        "Creating SparkSession for batch job.",
        extra={"app_name": app_name, "bucket": s3_cfg.bucket},
    )

    builder = (
        SparkSession.builder.appName(app_name)
        # S3A / MinIO config
        .config("spark.hadoop.fs.s3a.endpoint", s3_cfg.endpoint_url)
        .config("spark.hadoop.fs.s3a.access.key", s3_cfg.access_key)
        .config("spark.hadoop.fs.s3a.secret.key", s3_cfg.secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
        # Reasonable defaults for local batch runs
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.parquet.compression.codec", "snappy")
    )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession created.", extra={"app_name": app_name})
    return spark
