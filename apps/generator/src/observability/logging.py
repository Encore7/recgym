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

from .otlp_exporter import build_log_exporter

SERVICE_NAME_VALUE = os.getenv("OTEL_SERVICE_NAME", "recgym-generator")
ENVIRONMENT = os.getenv("OTEL_RESOURCE_ATTRIBUTES", "deployment.environment=local")


class JsonTraceFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        span = trace.get_current_span()
        span_ctx = span.get_span_context() if span else None
        payload: Dict[str, Any] = {
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "time": self.formatTime(record, datefmt="%Y-%m-%dT%H:%M:%S%z"),
            "trace_id": (
                format(span_ctx.trace_id, "032x")
                if span_ctx and span_ctx.is_valid
                else None
            ),
            "span_id": (
                format(span_ctx.span_id, "016x")
                if span_ctx and span_ctx.is_valid
                else None
            ),
            "service": SERVICE_NAME,
        }
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        for k, v in getattr(record, "__dict__", {}).items():
            if k.startswith("_") or k in payload or k in ("args", "msg"):
                continue
            try:
                json.dumps({k: v})
                payload[k] = v
            except Exception:
                pass
        return json.dumps(payload, separators=(",", ":"))


def init_logging(level: int = logging.INFO) -> None:
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setFormatter(JsonTraceFormatter())

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(stdout_handler)
    root.setLevel(level)

    attrs = dict(kv.split("=", 1) for kv in ENVIRONMENT.split(",") if "=" in kv)
    logger_provider = LoggerProvider(
        resource=Resource.create(
            {
                SERVICE_NAME: SERVICE_NAME_VALUE,
                **attrs,
            }
        )
    )
    processor = BatchLogRecordProcessor(build_log_exporter())
    logger_provider.add_log_record_processor(processor)

    handler = LoggingHandler(level=level, logger_provider=logger_provider)
    root.addHandler(handler)
