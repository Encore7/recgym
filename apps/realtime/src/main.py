"""
Entrypoint for the realtime PyFlink service.

For now this:
- initializes observability via the bootstrap
- constructs a TableEnvironment
- registers Kafka source/sinks
- wires user + item feature jobs
"""

from apps.realtime.src.core.bootstrap import bootstrap


def main() -> None:
    service = bootstrap()
    service.run()


if __name__ == "__main__":
    main()
