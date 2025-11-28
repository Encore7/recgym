"""
OpenTelemetry metric instruments for ingestion control-plane.

We track:
- Connector errors
- Poll/health-check latency
"""

from typing import Tuple

from opentelemetry.metrics import Counter, Histogram

from libs.observability.metrics import get_meter


def get_ingestion_instruments() -> Tuple[Counter, Histogram]:
    """
    Create OpenTelemetry instruments for ingestion service.

    Returns:
        A tuple containing:
            errors_counter: Counter for connector/health check errors.
            poll_latency_hist: Histogram for health check latency (ms).
    """
    meter = get_meter()

    errors_counter: Counter = meter.create_counter(
        name="ingestion_connector_errors",
        description="Count of ingestion connector/health-check errors",
        unit="1",
    )

    poll_latency_hist: Histogram = meter.create_histogram(
        name="ingestion_poll_latency_ms",
        description="Latency of ingestion health-check loop in milliseconds",
        unit="ms",
    )

    return errors_counter, poll_latency_hist
