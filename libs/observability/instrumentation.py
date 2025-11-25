"""
Observability bootstrap utilities for logging, tracing, and metrics.

This module provides:
- a unified initialization entrypoint (`init_observability`)
- stable producer/consumer metric instruments for Kafka
"""

import logging
from typing import Tuple

from opentelemetry.metrics import Counter, Histogram

from libs.observability.logging import init_logging
from libs.observability.metrics import get_meter, init_metrics
from libs.observability.tracing import init_tracing


def init_observability(level: int = logging.INFO) -> None:
    """
    Initialize logging, tracing, and metrics for the current service.

    This should typically be called once during service startup
    (e.g., in a bootstrap module or `if __name__ == "__main__"`).

    Args:
        level: Logging verbosity level for the root logger.
    """
    init_logging(level=level)
    init_tracing()
    init_metrics()


def get_producer_instruments() -> Tuple[Counter, Counter, Histogram]:
    """
    Create OpenTelemetry instruments for Kafka producers.

    Returns:
        A tuple containing:
            produced_counter: Counter for successfully produced events.
            failure_counter: Counter for failed produce attempts.
            latency_histogram: Histogram for event latency (ms).
    """
    meter = get_meter()

    produced: Counter = meter.create_counter(
        name="retail_events_produced",
        description="Count of successfully produced retail events",
        unit="1",
    )

    failures: Counter = meter.create_counter(
        name="retail_events_failed",
        description="Count of failed Kafka produce attempts",
        unit="1",
    )

    latency: Histogram = meter.create_histogram(
        name="retail_event_latency_ms",
        description="Kafka producer latency in milliseconds",
        unit="ms",
    )

    return produced, failures, latency


def get_consumer_instruments() -> Tuple[Counter, Counter, Histogram]:
    """
    Create OpenTelemetry instruments for Kafka consumers / ingestion.

    Returns:
        A tuple containing:
            consumed_counter: Counter for successfully consumed events.
            failure_counter: Counter for failed consumes/decodes.
            flush_latency_histogram: Histogram for flush-to-sink latency (ms).
    """
    meter = get_meter()

    consumed: Counter = meter.create_counter(
        name="retail_events_consumed",
        description="Count of successfully consumed retail events",
        unit="1",
    )

    failures: Counter = meter.create_counter(
        name="retail_events_consume_failed",
        description="Count of failed consume or decode operations",
        unit="1",
    )

    flush_latency: Histogram = meter.create_histogram(
        name="raw_events_flush_latency_ms",
        description="Latency of flushing ingested events to S3 or another sink",
        unit="ms",
    )

    return consumed, failures, flush_latency
