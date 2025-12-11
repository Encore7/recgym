from feast import FileSource

# Gold user features (partitioned by date)
gold_user_features_source = FileSource(
    name="gold_user_features_source",
    path="s3a://recgym-raw/gold/user_features",
    timestamp_field="date",  # partition column
    schema=None,  # use Parquet schema
)

# Gold item features (partitioned by date)
gold_item_features_source = FileSource(
    name="gold_item_features_source",
    path="s3a://recgym-raw/gold/item_features",
    timestamp_field="date",
    schema=None,
)

# Gold cross features (user_item)
gold_user_item_features_source = FileSource(
    name="gold_user_item_features_source",
    path="s3a://recgym-raw/gold/cross_features",
    timestamp_field="date",
    schema=None,
)