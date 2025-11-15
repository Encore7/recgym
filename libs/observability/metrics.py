"""
Metrics initialization and meter provider for OpenTelemetry.
"""

import os
from typing import Optional

from opentelemetry import metrics
from opentelemetry.metrics import Meter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.semconv.attributes.service_attributes import SERVICE_NAME

from libs.observability.otlp_exporter import build_metric_exporter

SERVICE_NAME_VALUE: str = os.getenv("OTEL_SERVICE_NAME", "recgym-service")
ENVIRONMENT: str = os.getenv("OTEL_RESOURCE_ATTRIBUTES", "deployment.environment=local")

_meter: Optional[Meter] = None


def init_metrics() -> None:
    """
    Initialize OTel metrics provider and exporter.
    """
    global _meter
    attrs = dict(kv.split("=", 1) for kv in ENVIRONMENT.split(",") if "=" in kv)
    resource = Resource.create({SERVICE_NAME: SERVICE_NAME_VALUE, **attrs})

    reader = PeriodicExportingMetricReader(build_metric_exporter())
    provider = MeterProvider(resource=resource, metric_readers=[reader])
    metrics.set_meter_provider(provider)
    _meter = metrics.get_meter(SERVICE_NAME_VALUE)


def get_meter() -> Meter:
    """
    Get the initialized Meter instance for the current service.

    Returns:
        A Meter instance.
    """
    if _meter is None:
        init_metrics()
    return metrics.get_meter(SERVICE_NAME_VALUE)
