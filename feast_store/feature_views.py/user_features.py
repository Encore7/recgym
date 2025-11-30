from feast import FeatureView, Field
from feast.types import Float32, Int32, Int64, String
from datetime import timedelta

from feast_store.entities import user
from feast_store.data_sources import user_features_source

user_features = FeatureView(
    name="user_features",
    entities=[user],
    ttl=timedelta(days=30),
    schema=[
        Field(name="user_total_events_30d", dtype=Int64),
        Field(name="user_views_30d", dtype=Int64),
        Field(name="user_add_to_cart_30d", dtype=Int64),
        Field(name="user_purchases_30d", dtype=Int64),

        Field(name="user_view_to_cart_rate_30d", dtype=Float32),
        Field(name="user_cart_to_purchase_rate_30d", dtype=Float32),

        Field(name="user_distinct_items_viewed_30d", dtype=Int32),
        Field(name="user_distinct_categories_viewed_30d", dtype=Int32),

        Field(name="user_avg_price_30d", dtype=Float32),
        Field(name="user_price_std_30d", dtype=Float32),
        Field(name="user_max_price_30d", dtype=Float32),
        Field(name="user_min_price_30d", dtype=Float32),

        Field(name="user_time_since_last_event_sec", dtype=Float32),
        Field(name="user_time_since_last_view_sec", dtype=Float32),
        Field(name="user_time_since_last_cart_sec", dtype=Float32),
        Field(name="user_time_since_last_purchase_sec", dtype=Float32),
        Field(name="user_days_since_first_event", dtype=Float32),

        Field(name="user_avg_session_length_events_30d", dtype=Float32),
        Field(name="user_median_session_length_events_30d", dtype=Float32),
        Field(name="user_sessions_7d", dtype=Int32),
        Field(name="user_events_last_session", dtype=Int32),

        Field(name="user_web_events_share_30d", dtype=Float32),
        Field(name="user_ios_events_share_30d", dtype=Float32),
        Field(name="user_android_events_share_30d", dtype=Float32),

        Field(name="user_morning_event_share_30d", dtype=Float32),
        Field(name="user_evening_event_share_30d", dtype=Float32),
        Field(name="user_night_event_share_30d", dtype=Float32),

        Field(name="user_top_category_1", dtype=String),
        Field(name="user_top_category_2", dtype=String),
        Field(name="user_top_category_3", dtype=String),

        # Embeddings (10 dims)
        *[
            Field(name=f"user_behavior_emb_{i}", dtype=Float32)
            for i in range(1, 11)
        ],
    ],
    source=user_features_source,
    online=True,
)
