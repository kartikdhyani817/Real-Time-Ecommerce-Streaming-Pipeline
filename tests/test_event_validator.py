from producer.event_generator import generate_event
from scripts.event_validator import validate_event


def test_valid_event_passes():
    event = generate_event()

    is_valid, errors = validate_event(event)

    assert is_valid is True
    assert errors == []


def test_negative_quantity_fails():
    event = generate_event()

    event["quantity"] = -1

    is_valid, errors = validate_event(event)

    assert is_valid is False
    assert len(errors) > 0


def test_missing_field_fails():
    event = generate_event()

    del event["order_id"]

    is_valid, errors = validate_event(event)

    assert is_valid is False

    assert any(
        "order_id" in error
        for error in errors
    )


def test_invalid_status_fails():
    event = generate_event()

    event["order_status"] = "BROKEN"

    is_valid, errors = validate_event(event)

    assert is_valid is False


def test_wrong_total_amount_fails():
    event = generate_event()

    event["total_amount"] = 1

    is_valid, errors = validate_event(event)

    assert is_valid is False


def test_invalid_timestamp_fails():
    event = generate_event()

    event["event_timestamp"] = "invalid-date"

    is_valid, errors = validate_event(event)

    assert is_valid is False