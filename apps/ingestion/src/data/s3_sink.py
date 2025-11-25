"""
S3/MinIO sink for raw Kafka events.

This sink buffers events in memory and periodically flushes them
as Parquet files to S3/MinIO, partitioned by date and hour.

Pattern:
- append(event: RetailEvent)  -> buffer
- flush()                     -> write Parquet → S3
"""

from __future__ import annotations

import io
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List

import boto3
import pyarrow as pa
import pyarrow.parquet as pq

from libs import RetailEvent


@dataclass
class S3SinkConfig:
    """
    Configuration for S3EventSink.

    Attributes:
        endpoint_url: S3/MinIO endpoint URL.
        access_key: Access key for S3/MinIO.
        secret_key: Secret key for S3/MinIO.
        bucket: Target bucket for writes.
        batch_size: Max in-memory buffer size before flush.
        flush_interval_sec: Max time between flushes.
        region: AWS-style region name (MinIO typically accepts "us-east-1").
    """

    endpoint_url: str
    access_key: str
    secret_key: str
    bucket: str
    batch_size: int
    flush_interval_sec: float
    region: str = "us-east-1"


class S3EventSink:
    """
    Buffers RetailEvent objects and periodically writes them to S3 as Parquet.

    Partitioning layout:
      s3://{bucket}/raw/year=YYYY/month=MM/day=DD/hour=HH/part-<timestamp>.parquet
    """

    def __init__(self, cfg: S3SinkConfig, logger: logging.Logger) -> None:
        """
        Initialize the S3 sink.

        Args:
            cfg: S3 sink configuration.
            logger: Logger instance for structured logging.
        """
        self._cfg = cfg
        self._log = logger
        self._buffer: List[RetailEvent] = []
        self._last_flush: float = time.time()

        self._s3 = boto3.client(
            "s3",
            endpoint_url=self._cfg.endpoint_url,
            aws_access_key_id=self._cfg.access_key,
            aws_secret_access_key=self._cfg.secret_key,
            region_name=self._cfg.region,
        )

        self._ensure_bucket()

    def _ensure_bucket(self) -> None:
        """
        Ensure the target bucket exists (idempotent).
        """
        try:
            self._s3.head_bucket(Bucket=self._cfg.bucket)
            self._log.info("S3 bucket exists", extra={"bucket": self._cfg.bucket})
        except Exception as exc:  # noqa: BLE001
            self._log.info(
                "Creating S3 bucket",
                extra={"bucket": self._cfg.bucket, "reason": str(exc)},
            )
            try:
                self._s3.create_bucket(Bucket=self._cfg.bucket)
            except Exception as create_exc:  # noqa: BLE001
                self._log.exception(
                    "Failed to create S3 bucket",
                    extra={"bucket": self._cfg.bucket},
                )
                raise RuntimeError("Unable to ensure S3 bucket.") from create_exc

    def append(self, event: RetailEvent) -> None:
        """
        Add one event to the in-memory buffer and flush if needed.

        Args:
            event: A single RetailEvent instance.
        """
        self._buffer.append(event)
        now = time.time()

        should_flush = (
            len(self._buffer) >= self._cfg.batch_size
            or (now - self._last_flush) >= self._cfg.flush_interval_sec
        )

        if should_flush:
            self.flush()

    def flush(self) -> None:
        """
        Serialize buffered events to Parquet and upload to S3.
        """
        if not self._buffer:
            return

        start = time.perf_counter()
        count = len(self._buffer)

        try:
            records = [event.model_dump() for event in self._buffer]
            table = pa.Table.from_pylist(records)

            now = datetime.now(tz=timezone.utc)
            key_prefix = (
                f"raw/year={now.year}/month={now.month:02d}/"
                f"day={now.day:02d}/hour={now.hour:02d}"
            )
            object_key = f"{key_prefix}/part-{int(now.timestamp())}.parquet"

            buf = io.BytesIO()
            pq.write_table(table, buf, compression="snappy")
            buf.seek(0)

            self._s3.put_object(
                Bucket=self._cfg.bucket,
                Key=object_key,
                Body=buf.getvalue(),
            )

            self._log.info(
                "Flushed events to S3",
                extra={
                    "count": count,
                    "bucket": self._cfg.bucket,
                    "key": object_key,
                },
            )
        except Exception as exc:  # noqa: BLE001
            self._log.exception(
                "Failed to flush events to S3",
                extra={"error": str(exc)},
            )
        finally:
            self._buffer.clear()
            self._last_flush = time.time()
            latency_ms = (time.perf_counter() - start) * 1000
            self._log.debug(
                "S3 flush latency",
                extra={"latency_ms": latency_ms, "count": count},
            )
