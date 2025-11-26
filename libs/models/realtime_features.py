# libs/models/realtime_features.py
"""
Pydantic models for realtime feature updates.

These mirror the logical schema of:
- user_features_rt (user-level windowed features)
- item_features_rt (item-level windowed features)

They are used by:
- rt-bridge (Kafka → Redis)
- API / model-serving to read from Redis
"""

from __future__ import annotations

from typing import ClassVar, Dict, Optional

from pydantic import BaseModel, Field


class UserFeatureUpdate(BaseModel):
    """
    Windowed user-level features over a short horizon (e.g. 5 minutes).
    """

    user_id: str
    session_len: int = Field(description="Number of events in the window")
    clicks_in_session: int = Field(
        description="Views + add_to_cart events in the window"
    )
    last_event_ts: int = Field(
        description="Timestamp (ms) of the last event in the window"
    )

    model_config: ClassVar[Dict[str, object]] = {
        "json_schema_extra": {
            "example": {
                "user_id": "u_12",
                "session_len": 7,
                "clicks_in_session": 5,
                "last_event_ts": 1731234567890,
            }
        }
    }


class ItemFeatureUpdate(BaseModel):
    """
    Windowed item-level features over a short horizon (e.g. 5 minutes).
    """

    item_id: str
    views_5m: int
    cart_5m: int
    purchases_5m: int
    avg_price_5m: float
    ts: int = Field(description="Representative window end timestamp (ms)")
    category_hint: Optional[str] = Field(
        default=None,
        description="Best-effort category string derived from events",
    )

    model_config: ClassVar[Dict[str, object]] = {
        "json_schema_extra": {
            "example": {
                "item_id": "i_51",
                "views_5m": 120,
                "cart_5m": 7,
                "purchases_5m": 2,
                "avg_price_5m": 29.99,
                "ts": 1731234567890,
                "category_hint": "Books",
            }
        }
    }
