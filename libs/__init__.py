"""
recgym shared library package.

This package contains:
- shared Pydantic models (e.g., events)
- observability utilities (logging, tracing, metrics)
- future shared config / base classes for services
"""

from libs.models.events import RetailEvent, DeviceType, EventType, PageType

__all__ = [
    "RetailEvent",
    "DeviceType",
    "EventType",
    "PageType",
]
