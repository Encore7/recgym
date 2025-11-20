import logging
import signal
import sys
import time
from typing import NoReturn

from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer
from opentelemetry import trace

from apps.ingestion.src.core.config import get_settings
from apps.ingestion.src.data.s3_sink import S3EventSink, S3SinkConfig
from libs.models.events import RetailEvent
from libs.observability.instrumentation import get_consumer_instruments


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
        self.settings = get_settings()
        self.log = logging.getLogger("KafkaToS3IngestionService")

        # Metrics
        self._consumed, self._failures, self._flush_latency = get_consumer_instruments()

        # Schema registry
        self._schema_registry_client = SchemaRegistryClient(
            {"url": self.settings.schema_registry_url}
        )

        schema_path = f"{self.settings.schema_dir}/RetailEvent.avsc"
        try:
            with open(schema_path, encoding="utf-8") as f:
                schema_str = f.read()
        except FileNotFoundError as exc:
            self.log.exception(
                "RetailEvent Avro schema file not found",
                extra={"schema_path": schema_path},
            )
            raise SystemExit(1) from exc

        def _to_retail_event(obj: dict, _: object) -> RetailEvent:
            return RetailEvent(**obj)

        self._value_deserializer = AvroDeserializer(
            schema_registry_client=self._schema_registry_client,
            schema_str=schema_str,
            from_dict=_to_retail_event,
        )

        # Kafka consumer
        self._consumer = DeserializingConsumer(
            {
                "bootstrap.servers": self.settings.kafka_bootstrap_servers,
                "key.deserializer": StringDeserializer("utf_8"),
                "value.deserializer": self._value_deserializer,
                "group.id": self.settings.kafka_group_id,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": True,
            }
        )

        # S3 sink
        sink_cfg = S3SinkConfig(
            endpoint_url=self.settings.s3_endpoint_url,
            access_key=self.settings.s3_access_key,
            secret_key=self.settings.s3_secret_key,
            bucket=self.settings.s3_bucket,
            region=self.settings.s3_region,
            batch_size=self.settings.batch_size,
            flush_interval_sec=self.settings.flush_interval_sec,
        )
        self._sink = S3EventSink(sink_cfg, logger=self.log)

        self._running = True

        # Signal handlers
        signal.signal(signal.SIGINT, self._stop)
        signal.signal(signal.SIGTERM, self._stop)

        # Tracer
        self._tracer = trace.get_tracer("recgym.ingestion")

    def _stop(self, *_: object) -> NoReturn:
        self.log.info("Shutdown signal received. Closing consumer.")
        self._running = False
        try:
            self._sink.flush()
            self._consumer.close()
        except Exception:
            self.log.exception("Error during ingestion shutdown")
        sys.exit(0)

    def run(self) -> None:
        """
        Main consumption loop.
        """
        self._consumer.subscribe([self.settings.kafka_topic])
        self.log.info(
            "Ingestion service started",
            extra={"topic": self.settings.kafka_topic},
        )

        while self._running:
            with self._tracer.start_as_current_span("poll_and_process"):
                msg = self._consumer.poll(1.0)
                if msg is None:
                    continue

                if msg.error():
                    self.log.error(
                        "Kafka message error",
                        extra={"error": str(msg.error())},
                    )
                    self._failures.add(1)
                    continue

                start = time.perf_counter()
                try:
                    event: RetailEvent = msg.value()
                    if event is None:
                        self.log.error("Deserialized event is None")
                        self._failures.add(1)
                        continue

                    self._sink.append(event)
                    self._consumed.add(1)
                except Exception as exc:
                    self.log.exception(
                        "Failed to process message",
                        extra={"error": str(exc)},
                    )
                    self._failures.add(1)
                finally:
                    latency_ms = (time.perf_counter() - start) * 1000
                    # We treat "flush latency" as a proxy for end-to-end processing latency
                    self._flush_latency.record(latency_ms)

        # normal exit path
        self.log.info("Ingestion loop stopped, flushing sink.")
        self._sink.flush()
        self._consumer.close()
