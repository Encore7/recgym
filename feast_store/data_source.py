from feast import FileSource
from batch.config import get_batch_paths

paths = get_batch_paths()

user_features_source = FileSource(
    name="gold_user_features_source",
    path=paths.gold_user_features,
    timestamp_field="date",
)

item_features_source = FileSource(
    name="gold_item_features_source",
    path=paths.gold_item_features,
    timestamp_field="date",
)

cross_features_source = FileSource(
    name="gold_cross_features_source",
    path=paths.gold_cross_features,
    timestamp_field="date",
)
