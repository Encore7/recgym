from datetime import timedelta

from feast import BatchSource, Entity, FeatureView, Field
from feast.types import Float64, Int64, String

item = Entity(
    name="item_id",
    value_type=String,
    description="Unique item identifier",
)

item_source = BatchSource(
    name="item_source",
    table="item_hourly_stats",
    timestamp_field="event_ts",
)

item_features = FeatureView(
    name="item_features",
    entities=[item],
    ttl=timedelta(days=7),
    schema=[
        Field(name="i_ctr_7d", dtype=Float64),
        Field(name="i_popularity_1h", dtype=Int64),
        Field(name="i_avg_price_7d", dtype=Float64),
        Field(name="i_primary_category", dtype=String),
    ],
    online=True,
    batch_source=item_source,
)
