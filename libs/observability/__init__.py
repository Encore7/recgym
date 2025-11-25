"""
Public observability interface for recgym services.

This module exposes stable entrypoints for:
- logging
- tracing
- metrics
- Kafka producer/consumer instruments

Services should import from here instead of the underlying modules
to keep the internal implementation swappable.
"""

from .instrumentation import (
    get_consumer_instruments,
    get_producer_instruments,
    init_observability,
)
from .logging import get_logger, init_logging
from .metrics import get_meter, init_metrics
from .tracing import get_tracer, init_tracing

__all__ = [
    "init_observability",
    "init_logging",
    "init_tracing",
    "init_metrics",
    "get_logger",
    "get_tracer",
    "get_meter",
    "get_producer_instruments",
    "get_consumer_instruments",
]
