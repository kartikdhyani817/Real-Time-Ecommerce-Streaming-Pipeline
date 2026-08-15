from datetime import datetime


REQUIRED_FIELDS = [
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


VALID_STATUSES = {
    "PLACED",
    "CONFIRMED",
    "PROCESSING",
}


def validate_event(event):
    """
    Validate one e-commerce order event.

    Returns:
        (True, []) when valid
        (False, [errors]) when invalid
    """

    errors = []

    # -----------------------------------------
    # Check event type
    # -----------------------------------------

    if not isinstance(event, dict):

        return False, [
            "Event must be a JSON object."
        ]

    # -----------------------------------------
    # Required fields
    # -----------------------------------------

    for field in REQUIRED_FIELDS:

        if field not in event:

            errors.append(
                f"Missing required field: {field}"
            )

        elif event[field] is None:

            errors.append(
                f"Null value detected: {field}"
            )

    # If important fields are missing,
    # avoid unnecessary validation errors
    # -----------------------------------------

    if errors:

        return False, errors

    # -----------------------------------------
    # Quantity validation
    # -----------------------------------------

    quantity = event["quantity"]

    if not isinstance(quantity, int):

        errors.append(
            "Quantity must be an integer."
        )

    elif quantity <= 0:

        errors.append(
            "Quantity must be greater than 0."
        )

    # -----------------------------------------
    # Price validation
    # -----------------------------------------

    unit_price = event["unit_price"]

    if not isinstance(
        unit_price,
        (int, float),
    ):

        errors.append(
            "Unit price must be numeric."
        )

    elif unit_price <= 0:

        errors.append(
            "Unit price must be greater than 0."
        )

    # -----------------------------------------
    # Total amount validation
    # -----------------------------------------

    total_amount = event["total_amount"]

    if not isinstance(
        total_amount,
        (int, float),
    ):

        errors.append(
            "Total amount must be numeric."
        )

    elif total_amount <= 0:

        errors.append(
            "Total amount must be greater than 0."
        )

    # -----------------------------------------
    # Business calculation validation
    # -----------------------------------------

    if (
        isinstance(quantity, int)
        and isinstance(
            unit_price,
            (int, float),
        )
        and isinstance(
            total_amount,
            (int, float),
        )
    ):

        expected_amount = (
            quantity * unit_price
        )

        if total_amount != expected_amount:

            errors.append(
                "Total amount does not match "
                "quantity × unit price."
            )

    # -----------------------------------------
    # Order status validation
    # -----------------------------------------

    if (
        event["order_status"]
        not in VALID_STATUSES
    ):

        errors.append(
            f"Invalid order status: "
            f"{event['order_status']}"
        )

    # -----------------------------------------
    # Timestamp validation
    # -----------------------------------------

    try:

        datetime.fromisoformat(
            event["event_timestamp"]
        )

    except (ValueError, TypeError):

        errors.append(
            "Invalid event timestamp."
        )

    return (
        len(errors) == 0,
        errors,
    )