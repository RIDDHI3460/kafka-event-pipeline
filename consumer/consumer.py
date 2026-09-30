"""
Consumer: reads order events from the Kafka topic and persists them
to a local SQLite database (and prints them to stdout), simulating a
downstream service that processes events off a message queue.
"""

import json
import os
import sqlite3
from datetime import datetime, timezone

from kafka import KafkaConsumer

TOPIC_NAME = os.environ.get("KAFKA_TOPIC", "order-events")
BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
DB_PATH = os.environ.get("DB_PATH", "orders.db")


def build_consumer(bootstrap_servers: str = BOOTSTRAP_SERVERS) -> KafkaConsumer:
    """Create and return a KafkaConsumer configured to read JSON messages."""
    return KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="order-processing-group",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
    )


def init_db(db_path: str = DB_PATH) -> sqlite3.Connection:
    """Create the orders table if it doesn't already exist."""
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS orders (
            order_id TEXT PRIMARY KEY,
            user_id INTEGER,
            item TEXT,
            quantity INTEGER,
            price REAL,
            event_timestamp TEXT,
            processed_at TEXT
        )
        """
    )
    conn.commit()
    return conn


def process_event(conn: sqlite3.Connection, event: dict) -> None:
    """Insert one order event into the database (idempotent on order_id)."""
    conn.execute(
        """
        INSERT OR REPLACE INTO orders
            (order_id, user_id, item, quantity, price, event_timestamp, processed_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            event["order_id"],
            event["user_id"],
            event["item"],
            event["quantity"],
            event["price"],
            event["timestamp"],
            datetime.now(timezone.utc).isoformat(),
        ),
    )
    conn.commit()


def run() -> None:
    conn = init_db()
    consumer = build_consumer()
    print(f"Listening on topic '{TOPIC_NAME}' at {BOOTSTRAP_SERVERS} ...")

    for message in consumer:
        event = message.value
        process_event(conn, event)
        print(f"processed -> partition={message.partition} "
              f"offset={message.offset} event={event}")


if __name__ == "__main__":
    run()
