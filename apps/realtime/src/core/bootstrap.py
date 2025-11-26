"""
Bootstrap for the realtime PyFlink processing service.

- Init observability
- Load global + realtime configs
- Create TableEnvironment
- Register Kafka source + sinks
- Attach user and item jobs
"""

from __future__ import annotations

import logging
from typing import Final

from pyflink.table import EnvironmentSettings, TableEnvironment

from libs.config import AppConfig
from libs.observability import init_observability

from apps.realtime.src.core.config import RealtimeSettings
from apps.realtime.src.infra.kafka import (
    build_events_source_ddl,
    build_user_features_sink_ddl,
    build_item_features_sink_ddl,
)
from apps.realtime.src.jobs.user_features_job import build_user_features_job
from apps.realtime.src.jobs.item_features_job import build_item_features_job


class RealtimeService:
    def __init__(self, settings: RealtimeSettings, app_config: AppConfig) -> None:
        self.settings = settings
        self.app_config = app_config
        self.log = logging.getLogger("RealtimeService")

        env_settings = EnvironmentSettings.in_streaming_mode()
        self.table_env: TableEnvironment = TableEnvironment.create(env_settings)

        # Configure Flink runtime
        cfg = self.table_env.get_config()
        cfg.set("parallelism.default", str(settings.parallelism))
        cfg.set("state.checkpoints.dir", settings.checkpoint_dir)

    def _register_tables(self) -> None:
        self.log.info("Registering Kafka source and sink tables")

        self.table_env.execute_sql(
            build_events_source_ddl(self.settings, self.app_config)
        )
        self.table_env.execute_sql(
            build_user_features_sink_ddl(self.settings, self.app_config)
        )
        self.table_env.execute_sql(
            build_item_features_sink_ddl(self.settings, self.app_config)
        )

    def run(self) -> None:
        self._register_tables()

        self.log.info("Attaching user-level job")
        build_user_features_job(self.table_env)

        self.log.info("Attaching item-level job")
        build_item_features_job(self.table_env)

        job_name: Final = "recgym-realtime-feature-pipeline"
        self.log.info("Submitting PyFlink job", extra={"job_name": job_name})
        self.table_env.execute(job_name)


def bootstrap() -> RealtimeService:
    init_observability()
    settings = RealtimeSettings.load()
    app_config = AppConfig.load()

    log = logging.getLogger("RealtimeBootstrap")
    log.info("Bootstrapping realtime PyFlink", extra={"parallelism": settings.parallelism})

    return RealtimeService(settings, app_config)
