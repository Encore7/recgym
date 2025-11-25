"""
BaseService: standard base class for all recgym services.

Provides:
- Logger
- Tracer
- Meter
- OTel bootstrap
- Structured error handling
- Standardized service lifecycle hooks

Every microservice should extend this class.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Optional

from libs.config import AppConfig
from libs.observability import (
    get_logger,
    get_tracer,
    get_meter,
    init_observability,
)


class BaseService(ABC):
    """
    Abstract base class for all services (generator, ingestion, API).

    Subclasses automatically receive:
    - `self.config`: Central AppConfig
    - `self.logger`: structured JSON logger
    - `self.tracer`: OpenTelemetry tracer
    - `self.meter`: OpenTelemetry metrics instance

    Subclasses must implement:
        async def start(self) -> None
        async def shutdown(self) -> None
    """

    def __init__(self, service_name: str):
        """
        Args:
            service_name: Logical name of the service (e.g. "generator").
        """
        # Load global configuration
        self.config = AppConfig.load()

        # Initialize observability stack if not already done
        init_observability(level=self._resolve_log_level())

        # Setup logger, tracer, meter
        self.logger = get_logger(service_name)
        self.tracer = get_tracer(service_name)
        self.meter = get_meter()

        self.logger.info(
            "Service initialized",
            extra={"service_name": service_name},
        )

    def _resolve_log_level(self) -> int:
        """Convert config log level string to numeric logging constant."""
        level_str = self.config.service.log_level.upper()
        return getattr(logging, level_str, logging.INFO)

    @abstractmethod
    async def start(self) -> None:
        """
        Start the service.

        Must be implemented by subclasses.
        """
        raise NotImplementedError

    @abstractmethod
    async def shutdown(self) -> None:
        """
        Clean up resources (Kafka connections, threads, etc.)

        Must be implemented by subclasses.
        """
        raise NotImplementedError

    # Optional convenience method used in generator/ingestion
    def run_sync(self) -> None:
        """
        Optional synchronous run helper for services that are async internally.

        This wraps async `start()` inside an event loop.
        """
        import asyncio

        try:
            asyncio.run(self.start())
        except Exception as exc:
            self.logger.error("Service crashed", exc_info=True)
            raise RuntimeError("Uncaught service failure") from exc
