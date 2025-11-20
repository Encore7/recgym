"""
S3/MinIO sink for raw Kafka events.

This sink buffers events in memory and periodically flushes them
as Parquet files to S3/MinIO, partitioned by date and hour.

Pattern:
- add(event: dict)  -> buffer
- flush()           -> write Parquet → S3
"""

import io
import logging
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import List

import boto3
import pyarrow as pa
import pyarrow.parquet as pq

from libs.models.events import RetailEvent


@dataclass
class S3SinkConfig:
    endpoint_url: str
    access_key: str
    secret_key: str
    bucket: str
    region: str
    batch_size: int
    flush_interval_sec: float


class S3EventSink:
    """
    Buffers RetailEvent objects and periodically writes them to S3 as Parquet.

    Partitioning layout:
      s3://{bucket}/raw/year=YYYY/month=MM/day=DD/hour=HH/part-<timestamp>.parquet
    """

    def __init__(self, cfg: S3SinkConfig, logger: logging.Logger) -> None:
        self._cfg = cfg
        self._log = logger
        self._buffer: List[RetailEvent] = []
        self._last_flush = time.time()

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
        except Exception:
            self._log.info("Creating S3 bucket", extra={"bucket": self._cfg.bucket})
            self._s3.create_bucket(Bucket=self._cfg.bucket)

    def append(self, event: RetailEvent) -> None:
        """
        Add one event to the in-memory buffer and flush if needed.
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
        try:
            # Convert to Arrow table
            records = [e.model_dump() for e in self._buffer]
            table = pa.Table.from_pylist(records)

            # Partition path based on current UTC time
            now = datetime.now(tz=timezone.utc)
            key_prefix = f"raw/year={now.year}/month={now.month:02d}/day={now.day:02d}/hour={now.hour:02d}"
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
                    "count": len(self._buffer),
                    "bucket": self._cfg.bucket,
                    "key": object_key,
                },
            )
        except Exception as exc:
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
                extra={"latency_ms": latency_ms},
            )
