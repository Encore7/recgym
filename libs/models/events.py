"""
Retail event model + supporting enums.

These models are shared between generator, ingestion, and downstream
feature pipelines. They define the typed contract for a single retail event.
"""

from enum import Enum
from typing import ClassVar, Dict, List, Optional

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
    """
    Typed retail event used by the generator and ingestion components.

    All timestamps are expressed as milliseconds since UNIX epoch.
    """

    user_id: str
    session_id: str
    item_id: str
    event_type: EventType
    price: float = Field(gt=0)
    category_path: List[str]
    device: DeviceType
    page: PageType
    referrer: Optional[str] = None
    ts: int  # milliseconds since UNIX epoch

    model_config: ClassVar[Dict[str, object]] = {
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
