import os
from typing import Dict, Optional

from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

DEFAULT_ENDPOINT = os.getenv(
    "OTEL_EXPORTER_OTLP_ENDPOINT", "http://otel-collector:4317"
)
DEFAULT_PROTOCOL = os.getenv("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc")


def _common_kwargs() -> Dict[str, object]:
    headers: Optional[str] = os.getenv("OTEL_EXPORTER_OTLP_HEADERS")
    kw = {
        "endpoint": DEFAULT_ENDPOINT,
        "headers": (
            dict([h.split("=") for h in headers.split(",")]) if headers else None
        ),
        "insecure": os.getenv("OTEL_EXPORTER_OTLP_INSECURE", "true").lower() == "true",
    }
    return kw


def build_trace_exporter() -> OTLPSpanExporter:
    return OTLPSpanExporter(**_common_kwargs())


def build_metric_exporter() -> OTLPMetricExporter:
    return OTLPMetricExporter(**_common_kwargs())


def build_log_exporter() -> OTLPLogExporter:
    return OTLPLogExporter(**_common_kwargs())
