"""
Entry point for the Kafka → S3 ingestion service.
"""

from core.bootstrap import bootstrap


def main() -> None:
    """
    Bootstrap and run the ingestion service.
    """
    service = bootstrap()
    service.run()


if __name__ == "__main__":
    main()
