"""
recgym shared library package.

This package contains:
- shared Pydantic models (e.g., events, realtime features)
- observability utilities (logging, tracing, metrics)
- future shared config / base classes for services
"""

from libs.models.events import RetailEvent, DeviceType, EventType, PageType
from libs.models.realtime_features import UserFeatureUpdate, ItemFeatureUpdate

__all__ = [
    "RetailEvent",
    "DeviceType",
    "EventType",
    "PageType",
    "UserFeatureUpdate",
    "ItemFeatureUpdate",
]
