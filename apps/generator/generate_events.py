import random
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

SCHEMA_REGISTRY_URL = "http://localhost:8081"
KAFKA_BROKER = "localhost:9092"
TOPIC = "events_raw"

# Schema Registry setup
schema_registry_conf = {"url": SCHEMA_REGISTRY_URL}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)
schema_str = open("infra/schemas/RetailEvent.avsc").read()
avro_serializer = AvroSerializer(schema_registry_client, schema_str)
producer_conf = {
    "bootstrap.servers": KAFKA_BROKER,
    "key.serializer": StringSerializer("utf_8"),
    "value.serializer": avro_serializer,
}
producer = SerializingProducer(producer_conf)

USERS = [f"u_{i}" for i in range(100)]
ITEMS = [f"i_{i}" for i in range(1000)]
EVENT_TYPES = ["view", "add_to_cart", "purchase"]
DEVICES = ["web", "ios", "android"]
PAGES = ["home", "plp", "pdp", "cart"]


def gen_event():
    return {
        "user_id": random.choice(USERS),
        "session_id": str(uuid.uuid4()),
        "item_id": random.choice(ITEMS),
        "event_type": random.choices(EVENT_TYPES, weights=[0.85, 0.1, 0.05])[0],
        "price": round(random.uniform(5, 200), 2),
        "category_path": [random.choice(["Electronics", "Fashion", "Books", "Home"])],
        "device": random.choice(DEVICES),
        "page": random.choice(PAGES),
        "referrer": random.choice(["campaign_1", "campaign_2", None]),
        "ts": int(datetime.now(tz=timezone.utc).timestamp() * 1000),
    }


if __name__ == "__main__":
    print(f"Streaming events to topic `{TOPIC}` ... Ctrl+C to stop")
    while True:
        event = gen_event()
        producer.produce(topic=TOPIC, key=event["user_id"], value=event)
        producer.poll(0)
        time.sleep(0.5)
