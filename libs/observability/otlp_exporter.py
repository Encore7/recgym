"""
Factory functions for OTLP exporters (gRPC logging, metrics, trace).
"""

import os
from typing import Dict, Optional

from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

DEFAULT_ENDPOINT: str = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel-collector:4317")
DEFAULT_HEADERS: Optional[str] = os.getenv("OTEL_EXPORTER_OTLP_HEADERS")


def _common_kwargs() -> Dict[str, object]:
    """
    Build common keyword arguments for all OTLP exporters.

    Returns:
        A dictionary containing endpoint, headers and insecure flag.
    """
    headers: Optional[Dict[str, str]] = (
        dict(h.split("=", 1) for h in DEFAULT_HEADERS.split(","))
        if DEFAULT_HEADERS
        else None
    )

    return {
        "endpoint": DEFAULT_ENDPOINT,
        "headers": headers,
        "insecure": True,
    }


def build_trace_exporter() -> OTLPSpanExporter:
    """
    Create and configure an OTLP trace exporter.

    Returns:
        An OTLPSpanExporter instance.
    """
    return OTLPSpanExporter(**_common_kwargs())


def build_metric_exporter() -> OTLPMetricExporter:
    """
    Create and configure an OTLP metric exporter.

    Returns:
        An OTLPMetricExporter instance.
    """
    return OTLPMetricExporter(**_common_kwargs())


def build_log_exporter() -> OTLPLogExporter:
    """
    Create and configure an OTLP log exporter.

    Returns:
        An OTLPLogExporter instance.
    """
    return OTLPLogExporter(**_common_kwargs())
