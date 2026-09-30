import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "producer"))

from producer import generate_event, ITEMS


def test_generate_event_has_required_fields():
    event = generate_event()
    required_fields = {"order_id", "user_id", "item", "quantity", "price", "timestamp"}
    assert required_fields.issubset(event.keys())


def test_generate_event_item_is_valid():
    event = generate_event()
    assert event["item"] in ITEMS


def test_generate_event_quantity_in_range():
    event = generate_event()
    assert 1 <= event["quantity"] <= 5


def test_generate_event_price_is_positive():
    event = generate_event()
    assert event["price"] > 0


def test_generate_event_order_ids_are_unique():
    ids = {generate_event()["order_id"] for _ in range(50)}
    # With 50 random UUID-derived ids, collisions should not happen
    assert len(ids) == 50
