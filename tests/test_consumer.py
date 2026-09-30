import sys
import os
import sqlite3
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "consumer"))

from consumer import init_db, process_event


SAMPLE_EVENT = {
    "order_id": "test1234",
    "user_id": 7,
    "item": "laptop",
    "quantity": 2,
    "price": 499.99,
    "timestamp": "2026-09-24T10:00:00+00:00",
}


def test_init_db_creates_orders_table():
    conn = init_db(":memory:")
    cursor = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='orders'"
    )
    assert cursor.fetchone() is not None


def test_process_event_inserts_row():
    conn = init_db(":memory:")
    process_event(conn, SAMPLE_EVENT)
    row = conn.execute(
        "SELECT order_id, item, quantity FROM orders WHERE order_id = ?",
        (SAMPLE_EVENT["order_id"],),
    ).fetchone()
    assert row == (SAMPLE_EVENT["order_id"], SAMPLE_EVENT["item"], SAMPLE_EVENT["quantity"])


def test_process_event_is_idempotent_on_order_id():
    conn = init_db(":memory:")
    process_event(conn, SAMPLE_EVENT)
    process_event(conn, SAMPLE_EVENT)  # same order_id sent twice
    count = conn.execute(
        "SELECT COUNT(*) FROM orders WHERE order_id = ?", (SAMPLE_EVENT["order_id"],)
    ).fetchone()[0]
    assert count == 1
