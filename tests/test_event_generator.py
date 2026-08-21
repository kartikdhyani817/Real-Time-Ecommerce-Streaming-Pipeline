from producer.event_generator import generate_event


def test_generate_event_returns_dict():
    event = generate_event()

    assert isinstance(event, dict)


def test_required_event_fields_exist():
    event = generate_event()

    required_fields = [
        "event_id",
        "event_type",
        "order_id",
        "customer_id",
        "product_id",
        "product_name",
        "category",
        "quantity",
        "unit_price",
        "total_amount",
        "payment_method",
        "order_status",
        "event_timestamp",
    ]

    for field in required_fields:
        assert field in event


def test_total_amount_calculation():
    event = generate_event()

    expected_amount = (
        event["quantity"]
        * event["unit_price"]
    )

    assert event["total_amount"] == expected_amount


def test_quantity_is_positive():
    event = generate_event()

    assert event["quantity"] > 0


def test_price_is_positive():
    event = generate_event()

    assert event["unit_price"] > 0