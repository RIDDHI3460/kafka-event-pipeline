"""
Producer: generates mock e-commerce order events and publishes them
to a Kafka topic.

Each event is a JSON document like:
{
    "order_id": "a1b2c3d4",
    "user_id": 42,
    "item": "laptop",
    "quantity": 1,
    "price": 999.99,
    "timestamp": "2026-09-24T10:00:00.000000"
}
"""

import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer

TOPIC_NAME = os.environ.get("KAFKA_TOPIC", "order-events")
BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

ITEMS = ["laptop", "phone", "headphones", "monitor", "keyboard", "mouse", "webcam"]


def build_producer(bootstrap_servers: str = BOOTSTRAP_SERVERS) -> KafkaProducer:
    """Create and return a KafkaProducer configured to send JSON messages."""
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )


def generate_event() -> dict:
    """Build a single random mock order event."""
    return {
        "order_id": str(uuid.uuid4())[:8],
        "user_id": random.randint(1, 100),
        "item": random.choice(ITEMS),
        "quantity": random.randint(1, 5),
        "price": round(random.uniform(9.99, 1499.99), 2),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def run(event_count: int = 20, delay_seconds: float = 1.0) -> None:
    """Continuously produce `event_count` events, one every `delay_seconds`."""
    producer = build_producer()
    print(f"Producing {event_count} events to topic '{TOPIC_NAME}' "
          f"on {BOOTSTRAP_SERVERS} ...")

    for i in range(event_count):
        event = generate_event()
        # Use order_id as the partition key so events for related orders
        # would land on the same partition (demonstrates ordering awareness).
        producer.send(TOPIC_NAME, key=event["order_id"], value=event)
        print(f"[{i + 1}/{event_count}] sent -> {event}")
        time.sleep(delay_seconds)

    producer.flush()
    producer.close()
    print("Done producing.")


if __name__ == "__main__":
    run()
