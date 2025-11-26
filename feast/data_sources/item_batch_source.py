from feast import FileSource

item_batch_source = FileSource(
    name="item_events_source",
    path="s3://recgym-raw/events/*.parquet",
    timestamp_field="ts",
    event_timestamp_column="ts",
)
