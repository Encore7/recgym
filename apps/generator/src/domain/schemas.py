"""
Logical mapping from Schema Registry subjects to local Avro files.

No I/O happens here; this is pure configuration used by infra.sche­mas and
infra.avro_serializer.
"""

from typing import Dict

# Subject names used in Schema Registry
RETAIL_EVENT_SUBJECT: str = "events_raw-value"
USER_FEATURE_SUBJECT: str = "features_user-value"
ITEM_FEATURE_SUBJECT: str = "features_item-value"

SUBJECT_TO_FILE: Dict[str, str] = {
    RETAIL_EVENT_SUBJECT: "RetailEvent.avsc",
    USER_FEATURE_SUBJECT: "UserFeatureUpdate.avsc",
    ITEM_FEATURE_SUBJECT: "ItemFeatureUpdate.avsc",
}
