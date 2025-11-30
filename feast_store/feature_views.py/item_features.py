from feast import FeatureView, Field
from feast.types import Float32, Int32, Int64, String
from datetime import timedelta

from feast_store.entities import item
from feast_store.data_sources import item_features_source

item_features = FeatureView(
    name="item_features",
    entities=[item],
    ttl=timedelta(days=30),
    schema=[
        Field(name="item_views_30d", dtype=Int64),
        Field(name="item_carts_30d", dtype=Int64),
        Field(name="item_purchases_30d", dtype=Int64),
        Field(name="item_ctr_30d", dtype=Float32),
        Field(name="item_cart_to_purchase_rate_30d", dtype=Float32),

        Field(name="item_avg_price_30d", dtype=Float32),
        Field(name="item_price_std_30d", dtype=Float32),
        Field(name="item_max_price_30d", dtype=Float32),
        Field(name="item_min_price_30d", dtype=Float32),

        Field(name="item_distinct_users_viewed_30d", dtype=Int32),
        Field(name="item_distinct_users_purchased_30d", dtype=Int32),

        Field(name="item_time_since_last_event_sec", dtype=Float32),
        Field(name="item_time_since_last_view_sec", dtype=Float32),
        Field(name="item_time_since_last_purchase_sec", dtype=Float32),

        Field(name="item_category_depth", dtype=Int32),
        Field(name="item_is_discounted_flag", dtype=Int32),

        Field(name="item_log_popularity_score_30d", dtype=Float32),

        Field(name="item_morning_view_share_30d", dtype=Float32),
        Field(name="item_evening_view_share_30d", dtype=Float32),

        # Embeddings
        *[
            Field(name=f"item_behavior_emb_{i}", dtype=Float32)
            for i in range(1, 11)
        ],
    ],
    source=item_features_source,
    online=True,
)
