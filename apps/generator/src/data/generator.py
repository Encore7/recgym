import random
import time
import uuid
from datetime import datetime, timezone
from typing import Generator, List

from ..core.models import DeviceType, EventType, GeneratorConfig, PageType, RetailEvent


class EventGenerator:
    """Generates realistic synthetic retail events using shared core models."""

    def __init__(self, cfg: GeneratorConfig) -> None:
        self.cfg = cfg
        self._rng = random.Random(cfg.seed)
        self._users = [f"u_{i}" for i in range(cfg.user_count)]
        self._items = [f"i_{i}" for i in range(cfg.item_count)]

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
        self._devices = [DeviceType.WEB, DeviceType.IOS, DeviceType.ANDROID]
        self._pages = [PageType.HOME, PageType.PLP, PageType.PDP, PageType.CART]

    def _now_ms(self) -> int:
        return int(datetime.now(tz=timezone.utc).timestamp() * 1000)

    def _price(self) -> float:
        return round(self._rng.uniform(self.cfg.price_min, self.cfg.price_max), 2)

    def _pick(self, seq):
        return self._rng.choice(seq)

    def _weighted_event_type(self) -> EventType:
        return self._rng.choices(self._event_types, weights=self._event_weights, k=1)[0]

    def _one(self, session_id: str) -> RetailEvent:
        return RetailEvent(
            user_id=self._pick(self._users),
            session_id=session_id,
            item_id=self._pick(self._items),
            event_type=self._weighted_event_type(),
            price=self._price(),
            category_path=[self._pick(self.cfg.categories)],
            device=self._pick(self._devices),
            page=self._pick(self._pages),
            referrer=self._pick(self.cfg.referrers),
            ts=self._now_ms(),
        )

    def session_events(self) -> List[RetailEvent]:
        """Generate a short session (2–6 events by default) under one session_id."""
        n = self._rng.randint(self.cfg.session_length_min, self.cfg.session_length_max)
        sid = str(uuid.uuid4())
        return [self._one(sid) for _ in range(n)]

    def stream(self) -> Generator[RetailEvent, None, None]:
        """Infinite event stream with pacing (used by producer)."""
        delay = max(self.cfg.base_event_delay_sec, 0.0)
        while True:
            for ev in self.session_events():
                yield ev
                if delay:
                    time.sleep(delay)


def create_generator(cfg: GeneratorConfig | None = None) -> EventGenerator:
    return EventGenerator(cfg or GeneratorConfig())
