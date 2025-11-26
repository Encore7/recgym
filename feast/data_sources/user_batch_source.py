from feast import FileSource
from datetime import timedelta

user_batch_source = FileSource(
    name="user_events_source",
    path="s3://recgym-raw/events/*.parquet",
    timestamp_field="ts",
    event_timestamp_column="ts",
)
