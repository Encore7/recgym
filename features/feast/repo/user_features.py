from datetime import timedelta

from feast import BatchSource, Entity, FeatureView, Field
from feast.types import Float64, Int64, String

# Entity definition
user = Entity(
    name="user_id",
    value_type=String,
    description="Unique user identifier",
)

# Batch source (Postgres is configured in feature_store.yaml)
user_source = BatchSource(
    name="user_source",
    table="user_daily_aggregates",
    timestamp_field="event_ts",
)

# Feature View
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
    batch_source=user_source,
)
