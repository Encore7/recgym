"""
Kafka Connect REST API client.

This client is used by the ingestion control-plane service to:
- Ensure the S3 sink connector exists
- Update connector configuration
- Fetch connector status
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional

import requests


class KafkaConnectClient:
    """
    Thin wrapper around Kafka Connect REST API.
    """

    def __init__(self, base_url: str, logger: logging.Logger, timeout_sec: float = 5.0) -> None:
        """
        Create a new KafkaConnectClient.

        Args:
            base_url: Base URL for Kafka Connect REST API (e.g. http://kafka-connect:8083).
            logger: Logger instance for structured logging.
            timeout_sec: Default timeout for HTTP requests, in seconds.
        """
        self._base_url = base_url.rstrip("/")
        self._log = logger
        self._timeout = timeout_sec

    def _url(self, path: str) -> str:
        """
        Build full URL for a relative REST path.

        Args:
            path: Relative REST path (e.g. "/connectors").

        Returns:
            Full URL string.
        """
        return f"{self._base_url}{path}"

    def get_connector(self, name: str) -> Optional[Dict[str, Any]]:
        """
        Fetch connector configuration.

        Args:
            name: Connector name.

        Returns:
            Connector JSON if it exists, else None.
        """
        url = self._url(f"/connectors/{name}")
        try:
            resp = requests.get(url, timeout=self._timeout)
        except requests.RequestException as exc:  # noqa: BLE001
            self._log.exception(
                "Failed to fetch connector",
                extra={"name": name, "url": url},
            )
            return None

        if resp.status_code == 200:
            return resp.json()

        if resp.status_code == 404:
            return None

        self._log.error(
            "Unexpected status code while fetching connector",
            extra={"name": name, "status_code": resp.status_code, "body": resp.text},
        )
        return None

    def ensure_s3_sink_connector(self, name: str, config: Dict[str, Any]) -> None:
        """
        Ensure that an S3 sink connector with the given name and config exists.

        If the connector does not exist, it is created.
        If it exists, its config is updated.

        Args:
            name: Connector name.
            config: Connector configuration map.
        """
        existing = self.get_connector(name)

        if existing is None:
            self._create_connector(name, config)
        else:
            self._update_connector_config(name, config)

    def _create_connector(self, name: str, config: Dict[str, Any]) -> None:
        url = self._url("/connectors")
        payload = {"name": name, "config": config}

        try:
            resp = requests.post(url, json=payload, timeout=self._timeout)
        except requests.RequestException as exc:  # noqa: BLE001
            self._log.exception(
                "Failed to create connector",
                extra={"name": name, "url": url},
            )
            raise RuntimeError("Failed to create connector.") from exc

        if resp.status_code not in (200, 201):
            self._log.error(
                "Connector creation failed",
                extra={"name": name, "status_code": resp.status_code, "body": resp.text},
            )
            raise RuntimeError("Connector creation failed.")

        self._log.info("Connector created", extra={"name": name})

    def _update_connector_config(self, name: str, config: Dict[str, Any]) -> None:
        url = self._url(f"/connectors/{name}/config")

        try:
            resp = requests.put(url, json=config, timeout=self._timeout)
        except requests.RequestException as exc:  # noqa: BLE001
            self._log.exception(
                "Failed to update connector config",
                extra={"name": name, "url": url},
            )
            raise RuntimeError("Failed to update connector config.") from exc

        if resp.status_code not in (200, 201):
            self._log.error(
                "Connector config update failed",
                extra={"name": name, "status_code": resp.status_code, "body": resp.text},
            )
            raise RuntimeError("Connector config update failed.")

        self._log.info("Connector config updated", extra={"name": name})

    def get_connector_status(self, name: str) -> Optional[Dict[str, Any]]:
        """
        Get connector status (tasks, state, worker id, etc.).

        Args:
            name: Connector name.

        Returns:
            Connector status JSON or None if unavailable.
        """
        url = self._url(f"/connectors/{name}/status")

        try:
            resp = requests.get(url, timeout=self._timeout)
        except requests.RequestException:  # noqa: BLE001
            self._log.exception(
                "Failed to fetch connector status",
                extra={"name": name, "url": url},
            )
            return None

        if resp.status_code == 200:
            return resp.json()

        self._log.error(
            "Unexpected status code while fetching connector status",
            extra={"name": name, "status_code": resp.status_code, "body": resp.text},
        )
        return None
