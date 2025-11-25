"""
OpenTelemetry tracing initialization and tracer helper.
"""

import os
from typing import Dict

from opentelemetry import trace
from opentelemetry.trace import Tracer
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.semconv.attributes.service_attributes import SERVICE_NAME

from libs.observability.otlp_exporter import build_trace_exporter

SERVICE_NAME_VALUE: str = os.getenv("OTEL_SERVICE_NAME", "recgym-service")
ENVIRONMENT: str = os.getenv(
    "OTEL_RESOURCE_ATTRIBUTES",
    "deployment.environment=local",
)


def init_tracing() -> None:
    """
    Initialize the global TracerProvider and configure the OTLP span exporter.

    This function is safe to call multiple times; subsequent calls replace
    the existing provider but existing spans will continue to export.
    """
    attrs: Dict[str, str] = {
        kv.split("=", 1)[0]: kv.split("=", 1)[1]
        for kv in ENVIRONMENT.split(",")
        if "=" in kv
    }
    resource = Resource.create({SERVICE_NAME: SERVICE_NAME_VALUE, **attrs})

    provider = TracerProvider(resource=resource)
    provider.add_span_processor(BatchSpanProcessor(build_trace_exporter()))
    trace.set_tracer_provider(provider)


def get_tracer(name: str | None = None) -> Tracer:
    """
    Get a Tracer instance for the given instrumentation scope.

    Args:
        name: Logical scope name for the tracer. If None, the service name is used.

    Returns:
        A Tracer instance.
    """
    scope_name = name or SERVICE_NAME_VALUE
    return trace.get_tracer(scope_name)
