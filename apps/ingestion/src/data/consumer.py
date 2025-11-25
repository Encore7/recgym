"""
Kafka → S3 ingestion service.

Consumes Avro-encoded RetailEvent messages from Kafka and writes them
as partitioned Parquet files to S3/MinIO, with OpenTelemetry metrics
and traces.
"""

from __future__ import annotations

import signal
import sys
import time
from typing import NoReturn

from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer

from libs import RetailEvent
from libs.config import AppConfig
from libs.observability import (
    get_consumer_instruments,
    get_logger,
    get_tracer,
)
from .s3_sink import S3EventSink, S3SinkConfig


class KafkaToS3IngestionService:
    """
    Kafka consumer that reads RetailEvent messages and writes them to S3 as Parquet.

    Responsibilities:
    - Subscribe to Kafka topic
    - Deserialize Avro payloads into RetailEvent
    - Buffer and flush to S3 via S3EventSink
    - Emit OTel metrics and traces
    """

    def __init__(self) -> None:
        self._config = AppConfig.load()
        self._logger = get_logger("KafkaToS3IngestionService")
        self._tracer = get_tracer("recgym.ingestion")

        # Metrics
        self._consumed_counter, self._failure_counter, self._flush_latency_hist = (
            get_consumer_instruments()
        )

        # Schema registry client
        self._schema_registry_client = SchemaRegistryClient(
            {"url": self._config.kafka.schema_registry_url}
        )

        # Load Avro schema
        schema_path = "infra/schemas/RetailEvent.avsc"
        try:
            with open(schema_path, encoding="utf-8") as f:
                schema_str = f.read()
        except FileNotFoundError as exc:
            self._logger.exception(
                "RetailEvent Avro schema file not found",
                extra={"schema_path": schema_path},
            )
            raise SystemExit(1) from exc
        except OSError as exc:
            self._logger.exception(
                "Error reading Avro schema file",
                extra={"schema_path": schema_path},
            )
            raise SystemExit(1) from exc

        schema = Schema(schema_str, "AVRO")

        def _to_retail_event(obj: dict, _: object) -> RetailEvent:
            return RetailEvent(**obj)

        self._value_deserializer = AvroDeserializer(
            schema_registry_client=self._schema_registry_client,
            schema_str=schema.schema_str,
            from_dict=_to_retail_event,
        )

        # Kafka consumer
        self._consumer = DeserializingConsumer(
            {
                "bootstrap.servers": self._config.kafka.bootstrap_servers,
                "key.deserializer": StringDeserializer("utf_8"),
                "value.deserializer": self._value_deserializer,
                "group.id": self._config.kafka.consumer_group,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": True,
            }
        )

        # S3 sink config
        sink_cfg = S3SinkConfig(
            endpoint_url=self._config.s3.endpoint_url,
            access_key=self._config.s3.access_key,
            secret_key=self._config.s3.secret_key,
            bucket=self._config.s3.bucket,
            batch_size=self._config.ingestion.batch_size,
            flush_interval_sec=self._config.ingestion.flush_interval_sec,
        )
        self._sink = S3EventSink(cfg=sink_cfg, logger=self._logger)

        self._running: bool = True

        # Signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._stop)
        signal.signal(signal.SIGTERM, self._stop)

    def _stop(self, *_: object) -> NoReturn:
        """
        Signal handler to flush pending data and close the consumer.
        """
        self._logger.info("Shutdown signal received. Stopping ingestion service.")
        self._running = False
        try:
            self._sink.flush()
            self._consumer.close()
        except Exception as exc:  # noqa: BLE001
            self._logger.exception(
                "Error during ingestion shutdown",
                extra={"error": str(exc)},
            )
        sys.exit(0)

    def run(self) -> None:
        """
        Main consumption loop: poll Kafka, deserialize messages, write to S3.
        """
        topic = self._config.kafka.input_topic
        self._consumer.subscribe([topic])

        self._logger.info(
            "Ingestion service started",
            extra={"topic": topic, "bootstrap": self._config.kafka.bootstrap_servers},
        )

        while self._running:
            with self._tracer.start_as_current_span("poll_and_process"):
                msg = self._consumer.poll(1.0)
                if msg is None:
                    continue

                if msg.error():
                    self._logger.error(
                        "Kafka message error",
                        extra={"error": str(msg.error())},
                    )
                    self._failure_counter.add(1)
                    continue

                start = time.perf_counter()
                try:
                    event: RetailEvent | None = msg.value()
                    if event is None:
                        self._logger.error("Deserialized event is None")
                        self._failure_counter.add(1)
                        continue

                    self._sink.append(event)
                    self._consumed_counter.add(1)
                except Exception as exc:  # noqa: BLE001
                    self._logger.exception(
                        "Failed to process message",
                        extra={"error": str(exc)},
                    )
                    self._failure_counter.add(1)
                finally:
                    latency_ms = (time.perf_counter() - start) * 1000
                    # We record processing latency as a proxy for flush latency here.
                    self._flush_latency_hist.record(latency_ms)

        # Normal exit path
        self._logger.info("Ingestion loop stopped, flushing sink.")
        self._sink.flush()
        self._consumer.close()
