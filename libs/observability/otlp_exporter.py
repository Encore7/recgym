"""
Factory methods for OTLP exporters (grpc logging, metrics, trace).
"""

import os
from typing import Dict, Optional

from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

DEFAULT_ENDPOINT: str = os.getenv(
    "OTEL_EXPORTER_OTLP_ENDPOINT",
    "http://otel-collector:4317",
)
DEFAULT_HEADERS: Optional[str] = os.getenv("OTEL_EXPORTER_OTLP_HEADERS")


def _common_kwargs() -> Dict[str, object]:
    headers: Optional[Dict[str, str]] = (
        dict(h.split("=") for h in DEFAULT_HEADERS.split(","))
        if DEFAULT_HEADERS
        else None
    )

    return {
        "endpoint": DEFAULT_ENDPOINT,
        "headers": headers,
        "insecure": True,
    }


def build_trace_exporter() -> OTLPSpanExporter:
    """Return configured trace exporter."""
    return OTLPSpanExporter(**_common_kwargs())


def build_metric_exporter() -> OTLPMetricExporter:
    """Return configured metric exporter."""
    return OTLPMetricExporter(**_common_kwargs())


def build_log_exporter() -> OTLPLogExporter:
    """Return configured log exporter."""
    return OTLPLogExporter(**_common_kwargs())
