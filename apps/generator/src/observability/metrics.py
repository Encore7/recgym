import os

from opentelemetry import metrics
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.semconv.attributes.service_attributes import SERVICE_NAME

from .otlp_exporter import build_metric_exporter

SERVICE_NAME_VALUE = os.getenv("OTEL_SERVICE_NAME", "recgym-generator")
ENVIRONMENT = os.getenv("OTEL_RESOURCE_ATTRIBUTES", "deployment.environment=local")


_meter = None


def init_metrics() -> None:
    global _meter
    attrs = dict(kv.split("=", 1) for kv in ENVIRONMENT.split(",") if "=" in kv)
    resource = Resource.create({SERVICE_NAME: SERVICE_NAME_VALUE, **attrs})

    reader = PeriodicExportingMetricReader(build_metric_exporter())
    provider = MeterProvider(resource=resource, metric_readers=[reader])
    metrics.set_meter_provider(provider)
    _meter = metrics.get_meter(SERVICE_NAME)


def get_meter():
    if _meter is None:
        init_metrics()
    return metrics.get_meter(SERVICE_NAME)
