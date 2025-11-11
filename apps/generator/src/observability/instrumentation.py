import logging

from .logging import init_logging
from .metrics import get_meter, init_metrics
from .tracing import init_tracing


def init_observability(log_level: int = logging.INFO) -> None:
    init_logging(level=log_level)
    init_tracing()
    init_metrics()


_produced_counter = None
_failure_counter = None
_latency_hist = None
_backlog_gauge = None


def get_producer_instruments():
    global _produced_counter, _failure_counter, _latency_hist, _backlog_gauge
    meter = get_meter()
    if _produced_counter is None:
        _produced_counter = meter.create_counter(
            name="retail_events_produced_total",
            description="Total events produced",
            unit="1",
        )
    if _failure_counter is None:
        _failure_counter = meter.create_counter(
            name="retail_events_produce_failures_total",
            description="Produce failures",
            unit="1",
        )
    if _latency_hist is None:
        _latency_hist = meter.create_histogram(
            name="retail_events_produce_latency_ms",
            description="Produce latency in milliseconds",
            unit="ms",
        )
    if _backlog_gauge is None:
        _backlog_val = {"value": 0.0}

        def _observe_callback(_):
            yield _backlog_val["value"], {}

        _backlog_gauge = meter.create_observable_gauge(
            name="retail_events_delivery_backlog",
            callbacks=[_observe_callback],
            description="In-flight messages backlog",
            unit="1",
        )

    def set_backlog(v: float):
        _backlog_val["value"] = float(v)
        _backlog_gauge.set = set_backlog  # type: ignore[attr-defined]

    return _produced_counter, _failure_counter, _latency_hist, _backlog_gauge
