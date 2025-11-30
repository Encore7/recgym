from feast import FeatureView, Field
from feast.types import Float32, Int32
from datetime import timedelta

from feast_store.entities import user_item
from feast_store.data_source import cross_features_source

cross_features = FeatureView(
    name="cross_features",
    entities=[user_item],
    ttl=timedelta(days=7),
    schema=[
        Field(name="ui_price_diff_from_user_avg", dtype=Float32),
        Field(name="ui_price_zscore_for_user", dtype=Float32),

        Field(name="ui_same_category_flag", dtype=Int32),
        Field(name="ui_same_top_category_flag", dtype=Int32),

        Field(name="ui_user_item_co_views_30d", dtype=Int32),
        Field(name="ui_user_item_co_carts_30d", dtype=Int32),
        Field(name="ui_user_item_co_purchases_30d", dtype=Int32),

        Field(name="ui_time_since_user_last_interaction_with_item_sec", dtype=Float32),

        Field(name="ui_position_in_session", dtype=Int32),
        Field(name="ui_session_event_count", dtype=Int32),

        Field(name="ui_recency_bucket", dtype=Int32),

        Field(name="ui_user_item_similarity_emb", dtype=Float32),
        Field(name="ui_user_item_exploration_score", dtype=Float32),
    ],
    source=cross_features_source,
    online=True,
)
