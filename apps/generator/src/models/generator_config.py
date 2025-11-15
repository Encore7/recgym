"""
Configuration for synthetic event generation.
"""

from typing import List, Optional

from pydantic import BaseModel


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
