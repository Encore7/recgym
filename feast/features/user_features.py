from feast import FeatureView, Field
from feast.types import Int64, Float32, String
from feast import FileSource
from datetime import timedelta

from apps.feast.data_sources.user_batch_source import user_batch_source
from apps.feast.features.entities import user

user_features = FeatureView(
    name="user_features",
    entities=[user],
    ttl=timedelta(days=30),
    schema=[
        Field(name="total_clicks_7d", dtype=Int64),
        Field(name="total_purchases_7d", dtype=Int64),
        Field(name="avg_session_length_7d", dtype=Float32),
        Field(name="device_preference", dtype=String),
    ],
    source=user_batch_source,
    online=True,
    offline=True,
)
