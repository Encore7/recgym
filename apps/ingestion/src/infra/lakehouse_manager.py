"""
Lakehouse manager for raw event storage.

This module is responsible for:
- Owning the raw lakehouse path configuration
- Optionally validating table format and structure

In this initial version, we only log and validate configuration.
You can extend this with Spark/Delta integration later.
"""

from __future__ import annotations

import logging


class LakehouseManager:
    """
    Manages lakehouse raw table configuration.

    In the current implementation, this only validates and logs configuration.
    """

    def __init__(self, raw_table_path: str, table_format: str, logger: logging.Logger) -> None:
        """
        Create a new LakehouseManager.

        Args:
            raw_table_path: Path to the raw table (e.g. s3://recgym-raw/raw/events).
            table_format: Table format (e.g. 'delta').
            logger: Logger instance for structured logging.
        """
        self._raw_table_path = raw_table_path
        self._table_format = table_format.lower()
        self._log = logger

    def ensure_raw_table(self) -> None:
        """
        Ensure that the raw table configuration is valid.

        In a future iteration, this method can:
        - Use Spark to create a Delta table if it does not exist.
        - Validate schema against RetailEvent.
        - Enforce partitioning (e.g. by ingestion_date).

        For now, we log and perform basic sanity checks.
        """
        if not self._raw_table_path:
            self._log.error("Raw table path is empty; cannot proceed.")
            raise RuntimeError("Invalid lakehouse raw table path.")

        if self._table_format not in {"delta", "iceberg", "hudi"}:
            self._log.warning(
                "Unknown table format; proceeding but downstream jobs may fail.",
                extra={"table_format": self._table_format},
            )

        self._log.info(
            "Lakehouse raw table configuration validated.",
            extra={
                "raw_table_path": self._raw_table_path,
                "table_format": self._table_format,
            },
        )
