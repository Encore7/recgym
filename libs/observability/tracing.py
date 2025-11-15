"""
OpenTelemetry tracing initialization.
"""

import os
from typing import Dict

from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.semconv.attributes.service_attributes import SERVICE_NAME

from libs.observability.otlp_exporter import build_trace_exporter

SERVICE_NAME_VALUE: str = os.getenv("OTEL_SERVICE_NAME", "recgym-service")
ENVIRONMENT: str = os.getenv("OTEL_RESOURCE_ATTRIBUTES", "deployment.environment=local")


def init_tracing() -> None:
    """
    Initialize tracing provider and exporter.
    """

    attrs: Dict[str, str] = dict(
        kv.split("=", 1) for kv in ENVIRONMENT.split(",") if "=" in kv
    )
    resource = Resource.create({SERVICE_NAME: SERVICE_NAME_VALUE, **attrs})

    provider = TracerProvider(resource=resource)
    provider.add_span_processor(BatchSpanProcessor(build_trace_exporter()))

    trace.set_tracer_provider(provider)
