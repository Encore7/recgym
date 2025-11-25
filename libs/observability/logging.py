"""
Structured JSON logging with OpenTelemetry Log Exporter.

This module configures:
- stdout JSON logs
- OpenTelemetry log pipeline (LoggerProvider + LogExporter)
- Trace/span correlation in every log line
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
ENVIRONMENT: str = os.getenv(
    "OTEL_RESOURCE_ATTRIBUTES",
    "deployment.environment=local",
)


class JsonTraceFormatter(logging.Formatter):
    """
    JSON formatter including trace_id and span_id.

    The formatter emits a single-line JSON object with:
        - level
        - logger
        - message
        - time
        - trace_id (hex) if available
        - span_id (hex) if available
        - service
        - additional contextual fields from the LogRecord.
    """

    def format(self, record: logging.LogRecord) -> str:
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

        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)

        # Include additional structured fields if they are JSON-serializable
        for key, value in record.__dict__.items():
            if key.startswith("_") or key in payload or key in ("args", "msg"):
                continue
            try:
                json.dumps({key: value})
            except Exception:
                continue
            payload[key] = value

        return json.dumps(payload, separators=(",", ":"))


def init_logging(level: int = logging.INFO) -> None:
    """
    Configure the root logger for the current process.

    This sets up:
        - JSON stdout logger
        - OpenTelemetry log pipeline via OTLP
        - Trace correlation via LoggingHandler

    Args:
        level: Minimum log level for the root logger.
    """
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setFormatter(JsonTraceFormatter())

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(stdout_handler)
    root.setLevel(level)
    root.propagate = False

    attrs = {
        kv.split("=", 1)[0]: kv.split("=", 1)[1]
        for kv in ENVIRONMENT.split(",")
        if "=" in kv
    }
    resource = Resource.create({SERVICE_NAME: SERVICE_NAME_VALUE, **attrs})

    logger_provider = LoggerProvider(resource=resource)
    logger_provider.add_log_record_processor(
        BatchLogRecordProcessor(build_log_exporter())
    )

    # OpenTelemetry log handler for root logger
    otel_handler = LoggingHandler(level=level, logger_provider=logger_provider)
    root.addHandler(otel_handler)


def get_logger(name: str | None = None) -> logging.Logger:
    """
    Get a module- or service-level logger.

    This helper is the canonical way for application code to obtain a logger.

    Args:
        name: Optional logger name. If None, the root logger is returned.

    Returns:
        A configured Logger instance.
    """
    return logging.getLogger(name)
