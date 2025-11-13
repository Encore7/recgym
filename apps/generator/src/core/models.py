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
    user_id: str
    session_id: str
    item_id: str
    event_type: EventType
    price: float = Field(gt=0)
    category_path: List[str]
    device: DeviceType
    page: PageType
    referrer: Optional[str] = None
    ts: int  # millis

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


class GeneratorConfig(BaseModel):
    user_count: int = 100
    item_count: int = 1000
    session_length_min: int = 2
    session_length_max: int = 6
    base_event_delay_sec: float = 0.5
    view_weight: float = 0.85
    add_to_cart_weight: float = 0.10
    purchase_weight: float = 0.05
    categories: List[str] = ["Electronics", "Fashion", "Books", "Home"]
    referrers: List[Optional[str]] = ["campaign_1", "campaign_2", None]
    price_min: float = 5.0
    price_max: float = 200.0
    seed: Optional[int] = None
