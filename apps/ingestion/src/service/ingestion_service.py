"""
Ingestion control-plane service.

Responsibilities:
- Ensure Kafka Connect S3 sink connector exists and is configured
- Validate lakehouse raw configuration
- Periodically check connector health and emit metrics
"""

from __future__ import annotations

import logging
import signal
import sys
import time
from typing import NoReturn

from libs.config import AppConfig
from libs.observability.tracing import get_tracer
from apps.ingestion.src.core.config import IngestionSettings
from apps.ingestion.src.infra.connect_client import KafkaConnectClient
from apps.ingestion.src.infra.lakehouse_manager import LakehouseManager
from apps.ingestion.src.infra.metrics import get_ingestion_instruments


class IngestionService:
    """
    Control-plane service for ingestion.

    This service does not move data itself. Instead, it:
    - Manages Kafka Connect S3 sink
    - Validates lakehouse configuration
    - Monitors connector health and emits metrics
    """

    def __init__(
        self,
        app_config: AppConfig,
        ingestion_cfg: IngestionSettings,
        connect_client: KafkaConnectClient,
        lakehouse_manager: LakehouseManager,
        logger: logging.Logger,
    ) -> None:
        """
        Create a new IngestionService.

        Args:
            app_config: Global application configuration (Kafka, S3, etc.).
            ingestion_cfg: Service-specific ingestion configuration.
            connect_client: Kafka Connect REST client.
            lakehouse_manager: Lakehouse manager for raw table.
            logger: Logger instance.
        """
        self._app_cfg = app_config
        self._cfg = ingestion_cfg
        self._connect_client = connect_client
        self._lakehouse_mgr = lakehouse_manager
        self._log = logger

        self._tracer = get_tracer("recgym.ingestion.service")
        self._errors_counter, self._poll_latency_hist = get_ingestion_instruments()

        self._running: bool = True
        signal.signal(signal.SIGINT, self._stop)
        signal.signal(signal.SIGTERM, self._stop)

    def _stop(self, *_: object) -> NoReturn:
        """
        Signal handler to stop the control-plane loop.
        """
        self._log.info("Shutdown signal received. Stopping ingestion service.")
        self._running = False
        sys.exit(0)

    def _build_s3_sink_config(self) -> dict:
        """
        Build Kafka Connect S3 sink connector configuration.

        Uses:
        - Global S3 config from AppConfig
        - Global Kafka config (bootstrap + topic)
        - Ingestion-specific prefix/table settings
        """
        kafka = self._app_cfg.kafka
        s3 = self._app_cfg.s3

        return {
            "connector.class": "io.confluent.connect.s3.S3SinkConnector",
            "tasks.max": "1",

            "topics": kafka.raw_topic,

            "key.converter": "org.apache.kafka.connect.storage.StringConverter",
            "value.converter": "io.confluent.connect.avro.AvroConverter",
            "value.converter.schema.registry.url": kafka.schema_registry_url,

            "s3.bucket.name": s3.bucket,
            "s3.region": "us-east-1",
            "store.url": s3.endpoint_url,

            "s3.credentials.provider.class": "io.confluent.connect.s3.auth.BasicCredentialsProvider",
            "aws.access.key.id": s3.access_key,
            "aws.secret.access.key": s3.secret_key,

            "flush.size": "1000",
            "rotate.interval.ms": "60000",

            "format.class": "io.confluent.connect.s3.format.parquet.ParquetFormat",
            "partitioner.class": "io.confluent.connect.storage.partitioner.TimeBasedPartitioner",
            "path.format": "'year'=YYYY/'month'=MM/'day'=dd/'hour'=HH",
            "partition.duration.ms": "3600000",
            "locale": "en_US",
            "timezone": "UTC",

            "storage.class": "io.confluent.connect.s3.storage.S3Storage",
            "topics.dir": self._cfg.s3_prefix,

            "behavior.on.null.values": "ignore",
            "schema.compatibility": "BACKWARD",
        }

    def _ensure_connector_and_lakehouse(self) -> None:
        """
        Ensure that:
        - The S3 sink connector exists and is configured
        - The lakehouse raw table configuration is valid
        """
        with self._tracer.start_as_current_span("ensure_connector_and_lakehouse"):
            self._log.info(
                "Ensuring S3 sink connector and lakehouse raw configuration.",
                extra={
                    "connector_name": self._cfg.connector_name,
                    "connect_url": self._cfg.connect_url,
                    "raw_table_path": self._cfg.raw_table_path,
                    "table_format": self._cfg.table_format,
                },
            )
            config = self._build_s3_sink_config()
            self._connect_client.ensure_s3_sink_connector(
                name=self._cfg.connector_name,
                config=config,
            )
            self._lakehouse_mgr.ensure_raw_table()
            self._log.info("Connector and lakehouse configuration ensured.")

    def _check_connector_health(self) -> None:
        """
        Check connector health and log status.

        Any errors are recorded via metrics.
        """
        with self._tracer.start_as_current_span("check_connector_health"):
            status = self._connect_client.get_connector_status(self._cfg.connector_name)
            if status is None:
                self._log.error(
                    "Connector status unavailable",
                    extra={"connector_name": self._cfg.connector_name},
                )
                self._errors_counter.add(1)
                return

            connector_state = status.get("connector", {}).get("state", "UNKNOWN")
            tasks = status.get("tasks", [])

            self._log.info(
                "Connector status",
                extra={
                    "connector_name": self._cfg.connector_name,
                    "state": connector_state,
                    "tasks": tasks,
                },
            )

            if connector_state.upper() != "RUNNING":
                self._errors_counter.add(1)

            for task in tasks:
                if task.get("state", "").upper() != "RUNNING":
                    self._errors_counter.add(1)

    def _startup_with_retry(self) -> None:
        """
        Startup phase with simple exponential backoff.

        Retries connector + lakehouse setup until success or process is killed.
        """
        attempt = 0
        while self._running:
            try:
                self._ensure_connector_and_lakehouse()
                return
            except Exception as exc:  # noqa: BLE001
                wait_sec = min(60.0, 5.0 * (2**attempt))
                self._errors_counter.add(1)
                self._log.exception(
                    "Failed to ensure connector/lakehouse during startup.",
                    extra={
                        "attempt": attempt + 1,
                        "wait_sec": wait_sec,
                        "error": str(exc),
                    },
                )
                time.sleep(wait_sec)
                attempt += 1

    def run(self) -> None:
        """
        Main control-loop for ingestion.

        - Ensures connector + lakehouse config on startup (with retry).
        - Periodically checks connector health and emits metrics.
        """
        self._log.info(
            "Ingestion control-plane service started.",
            extra={
                "connector_name": self._cfg.connector_name,
                "connect_url": self._cfg.connect_url,
            },
        )

        # Startup (connector + lakehouse) with retry/backoff
        self._startup_with_retry()

        poll_interval = max(self._cfg.poll_interval_sec, 1.0)

        while self._running:
            start = time.perf_counter()
            try:
                self._check_connector_health()
            except Exception as exc:  # noqa: BLE001
                self._log.exception(
                    "Unexpected error during connector health check.",
                    extra={"error": str(exc)},
                )
                self._errors_counter.add(1)
            finally:
                latency_ms = (time.perf_counter() - start) * 1000
                self._poll_latency_hist.record(latency_ms)

            time.sleep(poll_interval)
