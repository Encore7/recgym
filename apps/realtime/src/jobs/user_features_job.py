"""
PyFlink job to compute realtime user-level features.

It assumes the following tables are already registered:

- events_source      (Kafka source; see infra.kafka.build_events_source_ddl)
- user_features_rt   (Kafka sink; see infra.kafka.build_user_features_sink_ddl)
"""

from __future__ import annotations

from pyflink.table import TableEnvironment


def build_user_features_job(t_env: TableEnvironment) -> None:
    """
    Attach an INSERT statement computing user-level features over 5-minute windows.

    Features:
      - session_len: number of events in the window (proxy for session length)
      - clicks_in_session: views + add_to_cart in the window
      - last_event_ts: MAX(ts) in the window
    """
    t_env.execute_sql(
        """
        INSERT INTO user_features_rt
        SELECT
            user_id,
            COUNT(*) AS session_len,
            SUM(
                CASE
                    WHEN event_type IN ('view', 'add_to_cart') THEN 1
                    ELSE 0
                END
            ) AS clicks_in_session,
            MAX(ts) AS last_event_ts
        FROM events_source
        GROUP BY
            user_id,
            TUMBLE(rowtime, INTERVAL '5' MINUTE)
        """
    )
