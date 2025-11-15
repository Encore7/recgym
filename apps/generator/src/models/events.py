"""
Retail event model + supporting enums.
"""

from enum import Enum
from typing import List, Optional

from pydantic import BaseModel, Field


class EventType(str, Enum):
    VIEW = "view"
    ADD_TO_CART = "add_to_cart"
    PURCHASE = "purchase"


class DeviceType(str, Enum):
    WEB = "web"
    IOS = "ios"
    ANDROID = "android"


class PageType(str, Enum):
    HOME = "home"
    PLP = "plp"
    PDP = "pdp"
    CART = "cart"


class RetailEvent(BaseModel):
    """Typed retail event used by the generator."""

    user_id: str
    session_id: str
    item_id: str
    event_type: EventType
    price: float = Field(gt=0)
    category_path: List[str]
    device: DeviceType
    page: PageType
    referrer: Optional[str] = None
    ts: int  # milliseconds

    model_config = {
        "json_schema_extra": {
            "example": {
                "user_id": "u_12",
                "session_id": "f7b8c7d3-52d3-4781-8e7d-b87e2fd2f1f7",
                "item_id": "i_51",
                "event_type": "view",
                "price": 19.99,
                "category_path": ["Books"],
                "device": "web",
                "page": "home",
                "referrer": "campaign_1",
                "ts": 1731234567890,
            }
        }
    }
