"""
Observability bootstrap utilities for logging, tracing, and metrics.

This module provides a unified initialization entrypoint (`init_observability`)
and stable producer instruments that work with any service
without requiring service-specific changes.
"""

import logging
from typing import Tuple

from opentelemetry import metrics
from opentelemetry.metrics import Counter, Histogram

from libs.observability.logging import init_logging
from libs.observability.metrics import init_metrics
from libs.observability.tracing import init_tracing


def init_observability(level: int = logging.INFO) -> None:
    """
    Initialize logging, tracing, and metrics for the current service.

    Args:
        level: Logging verbosity level.
    """
    init_logging(level=level)
    init_tracing()
    init_metrics()


def get_producer_instruments() -> Tuple[Counter, Counter, Histogram]:
    """
    Create stable OpenTelemetry instruments for Kafka producers.

    Returns:
        A tuple containing:
            - produced_counter: Counter for successfully produced events.
            - failure_counter: Counter for failed produce attempts.
            - latency_histogram: Histogram for event latency (ms).
    """
    meter = metrics.get_meter("recgym.producer")

    produced = meter.create_counter(
        name="retail_events_produced",
        description="Count of successfully produced retail events",
        unit="1",
    )

    failures = meter.create_counter(
        name="retail_events_failed",
        description="Count of failed Kafka produce attempts",
        unit="1",
    )

    latency = meter.create_histogram(
        name="retail_event_latency_ms",
        description="Kafka producer latency in milliseconds",
        unit="ms",
    )

    return produced, failures, latency
