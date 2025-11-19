from datetime import timedelta

from feast import FeatureView, Field
from feast.infra.offline_stores.contrib.postgres_offline_store.postgres_source import (
    PostgresSource,
)
from feast.types import Float64, Int64, String

from features.feast.feature_repo.entities import user

user_source = PostgresSource(
    name="user_source",
    table="user_daily_aggregates",
    timestamp_field="event_ts",
)

user_features = FeatureView(
    name="user_features",
    entities=[user],
    ttl=timedelta(days=30),
    schema=[
        Field(name="u_clicks_7d", dtype=Int64),
        Field(name="u_buys_30d", dtype=Int64),
        Field(name="u_avg_price_30d", dtype=Float64),
        Field(name="u_primary_category", dtype=String),
        Field(name="u_last_seen_ts", dtype=Int64),
    ],
    online=True,
    source=user_source,
)
