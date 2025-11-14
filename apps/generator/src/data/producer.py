import logging
import signal
import sys
import time

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

from ..core.bootstrap import bootstrap
from ..core.config import get_settings
from ..observability.instrumentation import get_producer_instruments
from ..observability.tracing import trace


class KafkaEventProducer:
    def __init__(self):
        self.settings = get_settings()
        self.event_gen = bootstrap()
        self.log = logging.getLogger("KafkaEventProducer")

        # Observability counters
        self._produced, self._failures, self._latency, self._backlog = (
            get_producer_instruments()
        )
        self._running = True

        # Schema Registry
        self._schema_registry_client = SchemaRegistryClient(
            {"url": self.settings.schema_registry_url}
        )
        schema_path = f"{self.settings.schema_dir}/RetailEvent.avsc"
        with open(schema_path) as f:
            schema_str = f.read()

        avro_serializer = AvroSerializer(self._schema_registry_client, schema_str)

        # Kafka producer config
        self._producer = SerializingProducer(
            {
                "bootstrap.servers": self.settings.kafka_bootstrap_servers,
                "key.serializer": StringSerializer("utf_8"),
                "value.serializer": avro_serializer,
                "linger.ms": 50,
                "batch.size": 32768,
                "compression.type": "lz4",
            }
        )

        # Handle graceful stop
        signal.signal(signal.SIGINT, self._stop)
        signal.signal(signal.SIGTERM, self._stop)

    # Lifecycle
    def _stop(self, *_):
        self.log.info("Shutdown signal received. Flushing pending messages.")
        self._running = False
        self._producer.flush(10)
        sys.exit(0)

    # Producer loop
    def run(self) -> None:
        self.log.info("Kafka producer started.")
        tracer = trace.get_tracer("recgym-producer")

        for event in self.event_gen.stream():
            if not self._running:
                break

            start_time = time.perf_counter()
            with tracer.start_as_current_span("produce_event"):
                try:
                    self._producer.produce(
                        topic=self.settings.kafka_topic,
                        key=event.user_id,
                        value=event.model_dump(),  # Pydantic → dict
                    )
                    self._produced.add(1)
                except Exception as e:
                    self.log.error("Produce error", extra={"error": str(e)})
                    self._failures.add(1)
                finally:
                    latency_ms = (time.perf_counter() - start_time) * 1000
                    self._latency.record(latency_ms)
                    self._backlog.set(self._producer.len())

            # Poll delivery reports asynchronously
            self._producer.poll(0)

        self.log.info("Producer stopped cleanly.")


if __name__ == "__main__":
    KafkaEventProducer().run()
