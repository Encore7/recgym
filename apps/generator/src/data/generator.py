import random
import time
import uuid
from datetime import datetime, timezone
from typing import Generator, List, Sequence

from models.events import DeviceType, EventType, PageType, RetailEvent
from models.generator_config import GeneratorConfig


class EventGenerator:
    """
    Generates realistic synthetic retail events using shared core models.

    This is used by the Kafka producer to simulate a real-time stream of
    user-item interactions (view, add_to_cart, purchase) across devices
    and pages.
    """

    def __init__(self, cfg: GeneratorConfig) -> None:
        self.cfg: GeneratorConfig = cfg
        self._rng = random.Random(cfg.seed)

        # Pre-generate user and item pools for efficiency
        self._users: List[str] = [f"u_{i}" for i in range(cfg.user_count)]
        self._items: List[str] = [f"i_{i}" for i in range(cfg.item_count)]

        # Event types & weights
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

        # Devices & page types
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
        """Sample a random price in [price_min, price_max]."""
        return round(self._rng.uniform(self.cfg.price_min, self.cfg.price_max), 2)

    def _pick(self, seq: Sequence) -> object:
        """Randomly pick one element from a non-empty sequence."""
        return self._rng.choice(seq)

    def _weighted_event_type(self) -> EventType:
        """Sample an EventType according to configured weights."""
        return self._rng.choices(self._event_types, weights=self._event_weights, k=1)[0]

    def _one(self, session_id: str) -> RetailEvent:
        """Generate a single retail event within a given session."""
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
        """
        Generate a short session (2–6 events by default) under one session_id.

        Returns
        -------
        List[RetailEvent]
            A list of events sharing a session_id.
        """
        n_events = self._rng.randint(
            self.cfg.session_length_min,
            self.cfg.session_length_max,
        )
        session_id = str(uuid.uuid4())
        return [self._one(session_id) for _ in range(n_events)]

    def stream(self) -> Generator[RetailEvent, None, None]:
        """
        Infinite event stream with pacing.

        This is the primary API used by the Kafka producer: it yields events
        one-by-one with an optional inter-event delay to simulate real traffic.
        """
        delay = max(self.cfg.base_event_delay_sec, 0.0)

        while True:
            for event in self.session_events():
                yield event
                if delay:
                    time.sleep(delay)


def create_generator(cfg: GeneratorConfig | None = None) -> EventGenerator:
    """
    Convenience factory for an EventGenerator with default config when none is provided.
    """
    return EventGenerator(cfg or GeneratorConfig())
