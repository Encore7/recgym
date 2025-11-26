from feast import Entity

user = Entity(
    name="user_id",
    join_keys=["user_id"],
    description="User entity for recommendation system."
)

item = Entity(
    name="item_id",
    join_keys=["item_id"],
    description="Item entity for recommendation system."
)
