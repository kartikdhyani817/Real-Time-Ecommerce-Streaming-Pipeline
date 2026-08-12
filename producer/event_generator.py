import random
import uuid
from datetime import datetime, timezone

from faker import Faker


fake = Faker()


PRODUCTS = [
    {
        "product_id": "P001",
        "product_name": "Laptop",
        "category": "Electronics",
        "price": 75000,
    },
    {
        "product_id": "P002",
        "product_name": "Smartphone",
        "category": "Electronics",
        "price": 35000,
    },
    {
        "product_id": "P003",
        "product_name": "Headphones",
        "category": "Electronics",
        "price": 3500,
    },
    {
        "product_id": "P004",
        "product_name": "Keyboard",
        "category": "Electronics",
        "price": 2500,
    },
    {
        "product_id": "P005",
        "product_name": "Office Chair",
        "category": "Furniture",
        "price": 9000,
    },
    {
        "product_id": "P006",
        "product_name": "Desk",
        "category": "Furniture",
        "price": 12000,
    },
    {
        "product_id": "P007",
        "product_name": "Running Shoes",
        "category": "Fashion",
        "price": 4500,
    },
    {
        "product_id": "P008",
        "product_name": "Backpack",
        "category": "Fashion",
        "price": 2200,
    },
]


PAYMENT_METHODS = [
    "Credit Card",
    "Debit Card",
    "UPI",
    "Net Banking",
    "Cash on Delivery",
]


ORDER_STATUSES = [
    "PLACED",
    "CONFIRMED",
    "PROCESSING",
]


def generate_event():
    """
    Generate one simulated e-commerce order event.
    """

    product = random.choice(PRODUCTS)

    quantity = random.randint(1, 5)

    unit_price = product["price"]

    total_amount = quantity * unit_price

    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": "ORDER_CREATED",

        "order_id": (
            "ORD-"
            + uuid.uuid4().hex[:10].upper()
        ),

        "customer_id": (
            "CUS-"
            + uuid.uuid4().hex[:8].upper()
        ),

        "customer_name": fake.name(),

        "customer_email": fake.email(),

        "city": fake.city(),

        "country": fake.country(),

        "product_id": product["product_id"],

        "product_name": product["product_name"],

        "category": product["category"],

        "quantity": quantity,

        "unit_price": unit_price,

        "total_amount": total_amount,

        "payment_method": random.choice(
            PAYMENT_METHODS
        ),

        "order_status": random.choice(
            ORDER_STATUSES
        ),

        "event_timestamp": (
            datetime.now(timezone.utc).isoformat()
        ),
    }

    return event
