"""
Domain-level models used by the generator.

This module simply re-exports the shared RetailEvent domain model so that
the generator can depend on a stable, well-named import:

    from apps.generator.domain.models import RetailEvent
"""

from libs.models.events import (
    DeviceType,
    EventType,
    PageType,
    RetailEvent,
)

__all__ = ["RetailEvent", "DeviceType", "EventType", "PageType"]
