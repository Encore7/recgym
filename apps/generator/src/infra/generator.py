"""
Synthetic event generator implementation.

Generates realistic-looking retail events using shared domain models and
service-specific GeneratorSettings.
"""

import time
import uuid
from datetime import datetime, timezone
from typing import Generator, List, Sequence, TypeVar

from apps.generator.src.core.config import GeneratorSettings
from apps.generator.src.domain.models import (
    DeviceType,
    EventType,
    PageType,
    RetailEvent,
)

_T = TypeVar("_T")


class EventGenerator:
    """
    Generates synthetic retail events.

    The generator:
    - Samples a user and item from fixed pools
    - Picks event type, device, page, category, price
    - Emits streams of events grouped into sessions
    """

    def __init__(self, cfg: GeneratorSettings) -> None:
        """
        Create a new EventGenerator.

        Args:
            cfg: Validated generator settings.
        """
        self._cfg = cfg
        # We deliberately do not set a seed here; if you want deterministic
        # behavior, pass GENERATOR__SEED later and use it to seed random.Random.
        import random

        self._rng = random.Random(cfg.seed) if cfg.seed is not None else random.Random()

        self._users: List[str] = [f"u_{i}" for i in range(cfg.user_count)]
        self._items: List[str] = [f"i_{i}" for i in range(cfg.item_count)]

        self._event_types: List[EventType] = [
            EventType.VIEW,
            EventType.ADD_TO_CART,
            EventType.PURCHASE,
        ]
        self._event_weights: List[float] = [
            cfg.view_weight,
            cfg.add_to_cart_weight,
            cfg.purchase_weight,
        ]

        self._devices: List[DeviceType] = [
            DeviceType.WEB,
            DeviceType.IOS,
            DeviceType.ANDROID,
        ]
        self._pages: List[PageType] = [
            PageType.HOME,
            PageType.PLP,
            PageType.PDP,
            PageType.CART,
        ]

    @staticmethod
    def _now_ms() -> int:
        """Return current UTC time in epoch milliseconds."""
        return int(datetime.now(tz=timezone.utc).timestamp() * 1000)

    def _price(self) -> float:
        """Sample a random price within [price_min, price_max]."""
        return round(self._rng.uniform(self._cfg.price_min, self._cfg.price_max), 2)

    def _pick(self, seq: Sequence[_T]) -> _T:
        """
        Pick a random element from a non-empty sequence.

        Args:
            seq: Sequence to sample from.

        Returns:
            A single randomly chosen element.
        """
        return self._rng.choice(seq)

    def _weighted_event_type(self) -> EventType:
        """Sample an EventType according to configured weights."""
        return self._rng.choices(self._event_types, weights=self._event_weights, k=1)[0]

    def _one(self, session_id: str) -> RetailEvent:
        """
        Generate a single retail event within a given session.

        Args:
            session_id: The session identifier to attach to the event.

        Returns:
            A RetailEvent instance.
        """
        return RetailEvent(
            user_id=self._pick(self._users),
            session_id=session_id,
            item_id=self._pick(self._items),
            event_type=self._weighted_event_type(),
            price=self._price(),
            category_path=[self._pick(self._cfg.categories)],
            device=self._pick(self._devices),
            page=self._pick(self._pages),
            referrer=self._pick(self._cfg.referrers),
            ts=self._now_ms(),
        )

    def session_events(self) -> List[RetailEvent]:
        """
        Generate a short session (N events) under one session_id.

        Returns:
            A list of RetailEvent objects sharing a common session_id.
        """
        n_events = self._rng.randint(
            self._cfg.session_length_min,
            self._cfg.session_length_max,
        )
        session_id = str(uuid.uuid4())
        return [self._one(session_id) for _ in range(n_events)]

    def stream(self) -> Generator[RetailEvent, None, None]:
        """
        Infinite event stream with pacing.

        Yields:
            RetailEvent objects indefinitely, grouped by sessions.
        """
        delay = max(self._cfg.base_event_delay_sec, 0.0)

        while True:
            for event in self.session_events():
                yield event
                if delay:
                    time.sleep(delay)
