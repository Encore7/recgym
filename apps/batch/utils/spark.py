from __future__ import annotations

"""
Spark utilities for batch jobs.

Provides:
- get_spark_session: SparkSession configured for MinIO (S3A) and Delta Lake.
"""

import logging

from pyspark.sql import SparkSession

from libs.config import AppConfig
from libs.observability import get_logger

logger: logging.Logger = get_logger("batch.spark")


def get_spark_session(app_name: str) -> SparkSession:
    """
    Create a SparkSession configured for MinIO S3A access + Delta Lake.

    Uses:
        - AppConfig.s3.endpoint_url
        - AppConfig.s3.access_key
        - AppConfig.s3.secret_key

    Args:
        app_name: Logical name of the Spark application.

    Returns:
        Configured SparkSession instance.
    """
    config = AppConfig.load()
    s3_cfg = config.s3

    logger.info(
        "Creating SparkSession for batch job.",
        extra={"app_name": app_name, "bucket": s3_cfg.bucket},
    )

    # NOTE: Version pins may be adjusted to match your Spark image.
    delta_pkg = "io.delta:delta-spark_2.12:3.1.0"
    hadoop_aws_pkg = "org.apache.hadoop:hadoop-aws:3.3.4"
    aws_sdk_pkg = "com.amazonaws:aws-java-sdk-bundle:1.12.261"

    builder = (
        SparkSession.builder.appName(app_name)
        # Delta Lake integration
        .config("spark.jars.packages", f"{delta_pkg},{hadoop_aws_pkg},{aws_sdk_pkg}")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
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
