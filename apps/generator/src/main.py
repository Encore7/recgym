"""
Entrypoint for the generator service.
"""

import logging

from apps.generator.src.core.bootstrap import build_producer
from libs.observability.instrumentation import init_observability


def main() -> None:
    """
    Main entrypoint for the generator microservice.
    Initializes observability and runs the Kafka producer.
    """
    init_observability(level=logging.INFO)

    producer = build_producer()
    producer.run()


if __name__ == "__main__":
    main()
