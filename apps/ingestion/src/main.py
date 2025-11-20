from apps.ingestion.src.core.bootstrap import bootstrap


def main() -> None:
    service = bootstrap()
    service.run()


if __name__ == "__main__":
    main()
