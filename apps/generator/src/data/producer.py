import logging
import signal
import sys
import time
from typing import NoReturn

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
from core.bootstrap import bootstrap
from core.config import get_settings

from libs.observability.instrumentation import get_producer_instruments
from libs.observability.tracing import trace


class KafkaEventProducer:
    """
    Kafka producer for streaming synthetic retail events.

    Responsibilities:
    - Bootstrap generator (topics, schemas, observability).
    - Serialize events using Avro and Schema Registry.
    - Produce records to Kafka with basic metrics instrumentation.
    """

    def __init__(self) -> None:
        self.settings = get_settings()
        self.event_gen = bootstrap()
        self.log = logging.getLogger("KafkaEventProducer")

        # OTel metrics instruments (counters + histogram)
        self._produced, self._failures, self._latency = get_producer_instruments()

        self._running: bool = True

        # Schema Registry client
        self._schema_registry_client = SchemaRegistryClient(
            {"url": self.settings.schema_registry_url}
        )

        # Load Avro schema from infra/schemas
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

        # Avro serializer for the event value
        self._value_serializer = AvroSerializer(
            schema_registry_client=self._schema_registry_client,
            schema_str=schema_str,
        )

        # Kafka producer
        self._producer = SerializingProducer(
            {
                "bootstrap.servers": self.settings.kafka_bootstrap_servers,
                "key.serializer": StringSerializer("utf_8"),
                "value.serializer": self._value_serializer,
                "compression.type": "lz4",
                "linger.ms": 50,
                "batch.size": 32768,
            }
        )

        # Graceful shutdown
        signal.signal(signal.SIGINT, self._stop)
        signal.signal(signal.SIGTERM, self._stop)

    def _stop(self, *_: object) -> NoReturn:
        """
        Signal handler to flush pending messages and exit cleanly.
        """
        self.log.info("Shutdown signal received. Flushing pending messages.")
        self._running = False
        try:
            self._producer.flush(10)
        except Exception:
            # We log but still exit – shutdown should not hang.
            self.log.exception("Error while flushing producer during shutdown")
        sys.exit(0)

    def run(self) -> None:
        """
        Main producer loop – streams events from EventGenerator into Kafka.
        """
        self.log.info("Kafka producer started.")
        tracer = trace.get_tracer("recgym-producer")

        for event in self.event_gen.stream():
            if not self._running:
                break

            start = time.perf_counter()

            with tracer.start_as_current_span("produce_event"):
                try:
                    self._producer.produce(
                        topic=self.settings.kafka_topic,
                        key=event.user_id,
                        value=event.model_dump(),  # Pydantic → dict
                    )
                    self._produced.add(1)
                except Exception as exc:
                    self.log.error(
                        "Produce error",
                        extra={"error": str(exc), "topic": self.settings.kafka_topic},
                    )
                    self._failures.add(1)
                finally:
                    latency_ms = (time.perf_counter() - start) * 1000
                    self._latency.record(latency_ms)

            # Trigger delivery report callbacks
            self._producer.poll(0)

        # Normal shutdown path if loop exits naturally
        self.log.info("Producer stopped cleanly.")
        try:
            self._producer.flush(10)
        except Exception:
            self.log.exception("Error during final producer flush")


if __name__ == "__main__":
    KafkaEventProducer().run()
