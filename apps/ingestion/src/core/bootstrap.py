"""
Bootstrap for the ingestion control-plane service.

Responsibilities:
- Load global AppConfig and IngestionSettings
- Construct KafkaConnectClient and LakehouseManager
- Build the IngestionService instance
"""

from typing import Final

from libs.config import AppConfig
from libs.observability import get_logger  # assuming __init__ re-exports
from apps.ingestion.src.core.config import IngestionSettings, get_ingestion_settings
from apps.ingestion.src.infra.connect_client import KafkaConnectClient
from apps.ingestion.src.infra.lakehouse_manager import LakehouseManager
from apps.ingestion.src.service.ingestion_service import IngestionService


def bootstrap() -> IngestionService:
    """
    Build a fully wired IngestionService instance.

    Returns:
        IngestionService: Configured control-plane service.
    """
    log = get_logger("ingestion-bootstrap")
    log.info("Bootstrapping ingestion control-plane service")

    app_cfg: Final[AppConfig] = AppConfig.load()
    ing_cfg: Final[IngestionSettings] = get_ingestion_settings()

    connect_client = KafkaConnectClient(
        base_url=ing_cfg.connect_url,
        logger=get_logger("KafkaConnectClient"),
    )

    lakehouse_mgr = LakehouseManager(
        raw_table_path=ing_cfg.raw_table_path,
        table_format=ing_cfg.table_format,
        logger=get_logger("LakehouseManager"),
    )

    service = IngestionService(
        app_config=app_cfg,
        ingestion_cfg=ing_cfg,
        connect_client=connect_client,
        lakehouse_manager=lakehouse_mgr,
        logger=get_logger("IngestionService"),
    )

    log.info("Ingestion service initialized")
    return service
