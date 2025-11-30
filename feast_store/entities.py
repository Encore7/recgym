from feast import Entity

user = Entity(
    name="user",
    join_keys=["user_id"],
    description="User entity.",
)

item = Entity(
    name="item",
    join_keys=["item_id"],
    description="Item entity.",
)

user_item = Entity(
    name="user_item",
    join_keys=["user_id", "item_id"],
    description="User–item cross entity.",
)
