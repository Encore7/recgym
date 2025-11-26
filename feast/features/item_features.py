from feast import FeatureView, Field
from feast.types import Int64, Float32, String
from datetime import timedelta

from apps.feast.data_sources.item_batch_source import item_batch_source
from apps.feast.features.entities import item

item_features = FeatureView(
    name="item_features",
    entities=[item],
    ttl=timedelta(days=30),
    schema=[
        Field(name="views_24h", dtype=Int64),
        Field(name="purchases_24h", dtype=Int64),
        Field(name="avg_price_24h", dtype=Float32),
        Field(name="primary_category", dtype=String),
    ],
    source=item_batch_source,
    online=True,
    offline=True,
)
