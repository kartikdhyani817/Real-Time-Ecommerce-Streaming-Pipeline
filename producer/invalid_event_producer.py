import json

from kafka import KafkaProducer


KAFKA_SERVER = "localhost:9092"

TOPIC_NAME = "ecommerce-orders"


producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,

    value_serializer=lambda value: (
        json.dumps(value)
        .encode("utf-8")
    ),
)


invalid_event = {
    "event_id": "INVALID-001",

    "event_type": "ORDER_CREATED",

    "order_id": "ORD-INVALID",

    "customer_id": "CUS-INVALID",

    "product_id": "P001",

    "product_name": "Laptop",

    "category": "Electronics",

    # Intentionally invalid
    "quantity": -5,

    "unit_price": 75000,

    # Wrong amount
    "total_amount": 100,

    "payment_method": "UPI",

    # Invalid status
    "order_status": "BROKEN",

    "event_timestamp": (
        "not-a-valid-timestamp"
    ),
}


producer.send(
    TOPIC_NAME,
    value=invalid_event,
)

producer.flush()

producer.close()


print(
    "Invalid test event sent to Kafka."
)