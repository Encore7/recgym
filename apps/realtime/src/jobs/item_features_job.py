"""
PyFlink job to compute realtime item-level features.

It assumes the following tables are already registered:

- events_source      (Kafka source; see infra.kafka.build_events_source_ddl)
- item_features_rt   (Kafka sink; see infra.kafka.build_item_features_sink_ddl)
"""

from __future__ import annotations

from pyflink.table import TableEnvironment


def build_item_features_job(t_env: TableEnvironment) -> None:
    """
    Attach an INSERT statement computing item-level features over 5-minute windows.

    Features:
      - views_5m:      COUNT of events where event_type = 'view'
      - cart_5m:       COUNT where event_type = 'add_to_cart'
      - purchases_5m:  COUNT where event_type = 'purchase'
      - avg_price_5m:  AVG(price) over the window
      - ts:            MAX(ts) as representative window end timestamp
      - category_hint: FIRST_VALUE of main category (best-effort)
    """
    t_env.execute_sql(
        """
        INSERT INTO item_features_rt
        SELECT
            item_id,
            SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) AS views_5m,
            SUM(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) AS cart_5m,
            SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS purchases_5m,
            AVG(price) AS avg_price_5m,
            MAX(ts) AS ts,
            -- Best-effort category hint: pick the first category_path[1]
            ELEMENT_AT(
                ARRAY_AGG(
                    CASE
                        WHEN CARDINALITY(category_path) > 0
                        THEN category_path[1]
                        ELSE NULL
                    END
                    IGNORE NULLS
                ),
                1
            ) AS category_hint
        FROM events_source
        GROUP BY
            item_id,
            TUMBLE(rowtime, INTERVAL '5' MINUTE)
        """
    )
