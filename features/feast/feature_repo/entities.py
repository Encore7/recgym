from feast import Entity
from feast.types import String

user = Entity(
    name="user_id",
    value_type=String,
    description="Unique user identifier",
)

item = Entity(
    name="item_id",
    value_type=String,
    description="Unique item identifier",
)
