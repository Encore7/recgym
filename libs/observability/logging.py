"""
Structured JSON logging with OpenTelemetry Log Exporter.

This module attaches:
- stdout JSON logs
- OTel LogProcessor + LogExporter
- Trace/span correlation
"""

import json
import logging
import os
import sys
from typing import Any, Dict

from opentelemetry import trace
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.resources import Resource
from opentelemetry.semconv.attributes.service_attributes import SERVICE_NAME

from libs.observability.otlp_exporter import build_log_exporter

SERVICE_NAME_VALUE: str = os.getenv("OTEL_SERVICE_NAME", "recgym-service")
ENVIRONMENT: str = os.getenv("OTEL_RESOURCE_ATTRIBUTES", "deployment.environment=local")


class JsonTraceFormatter(logging.Formatter):
    """
    JSON formatter including trace_id and span_id.
    """

    def format(self, record: logging.LogRecord) -> str:  # type: ignore[override]
        span = trace.get_current_span()
        span_ctx = span.get_span_context() if span else None

        payload: Dict[str, Any] = {
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "time": self.formatTime(record, "%Y-%m-%dT%H:%M:%S%z"),
            "trace_id": (
                f"{span_ctx.trace_id:032x}" if span_ctx and span_ctx.is_valid else None
            ),
            "span_id": (
                f"{span_ctx.span_id:016x}" if span_ctx and span_ctx.is_valid else None
            ),
            "service": SERVICE_NAME_VALUE,
        }

        # Include exception info if present
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)

        # Include additional structured fields
        for k, v in record.__dict__.items():
            if k.startswith("_") or k in payload or k in ("args", "msg"):
                continue
            try:
                json.dumps({k: v})
                payload[k] = v
            except Exception:
                pass

        return json.dumps(payload, separators=(",", ":"))


def init_logging(level: int = logging.INFO) -> None:
    """
    Configure root logger with:
    - JSON stdout logging
    - OTel log exporter
    - Trace correlation
    """

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setFormatter(JsonTraceFormatter())

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(stdout_handler)
    root.setLevel(level)
    root.propagate = False

    attrs = dict(kv.split("=", 1) for kv in ENVIRONMENT.split(",") if "=" in kv)
    resource = Resource.create({SERVICE_NAME: SERVICE_NAME_VALUE, **attrs})

    logger_provider = LoggerProvider(resource=resource)
    logger_provider.add_log_record_processor(
        BatchLogRecordProcessor(build_log_exporter())
    )

    # OpenTelemetry log handler
    handler = LoggingHandler(level=level, logger_provider=logger_provider)
    root.addHandler(handler)
