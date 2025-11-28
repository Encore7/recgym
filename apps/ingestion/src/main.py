"""
Entrypoint for the ingestion control-plane service.

This service orchestrates:
- Kafka Connect S3 sink for raw events
- Lakehouse raw path/table lifecycle
- Basic health/metrics around ingestion
"""

import logging

from apps.ingestion.src.core.bootstrap import bootstrap
from libs.observability.instrumentation import init_observability


def main() -> None:
    """
    Initialize observability and run the ingestion service.
    """
    init_observability(level=logging.INFO)

    service = bootstrap()
    service.run()


if __name__ == "__main__":
    main()
