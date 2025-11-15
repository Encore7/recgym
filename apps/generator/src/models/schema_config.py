"""
Schema registry configuration + subject/file mapping.
"""

from pydantic import BaseModel


class SchemaRegistryConfig(BaseModel):
    url: str
    schema_dir: str

    # subject → avro file
    subjects: dict = {
        "events_raw-value": "RetailEvent.avsc",
        "features_user-value": "UserFeatureUpdate.avsc",
        "features_item-value": "ItemFeatureUpdate.avsc",
    }
