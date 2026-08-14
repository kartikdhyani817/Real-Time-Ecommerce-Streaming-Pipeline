import json
from pathlib import Path

from kafka import KafkaConsumer


KAFKA_SERVER = "localhost:9092"

TOPIC_NAME = "ecommerce-orders"

GROUP_ID = "ecommerce-processing-group"

OUTPUT_DIR = Path("data/processed")

OUTPUT_FILE = OUTPUT_DIR / "consumed_events.jsonl"


def create_consumer():
    """
    Create and configure the Kafka consumer.
    """

    consumer = KafkaConsumer(
        TOPIC_NAME,

        bootstrap_servers=KAFKA_SERVER,

        group_id=GROUP_ID,

        auto_offset_reset="earliest",

        enable_auto_commit=True,

        value_deserializer=lambda message: json.loads(
            message.decode("utf-8")
        ),
    )

    return consumer


def save_event(event):
    """
    Save consumed event locally as JSONL.
    """

    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    with OUTPUT_FILE.open(
        "a",
        encoding="utf-8",
    ) as file:

        file.write(
            json.dumps(event)
            + "\n"
        )


def consume_events():
    """
    Continuously consume e-commerce events from Kafka.
    """

    print("=" * 65)
    print("E-Commerce Kafka Consumer")
    print("=" * 65)

    print(
        f"\nKafka Server : {KAFKA_SERVER}"
    )

    print(
        f"Topic        : {TOPIC_NAME}"
    )

    print(
        f"Consumer Group: {GROUP_ID}"
    )

    print(
        "\nWaiting for events...\n"
    )

    consumer = create_consumer()

    event_count = 0

    try:

        for message in consumer:

            event_count += 1

            event = message.value

            print("-" * 65)

            print(
                f"EVENT #{event_count}"
            )

            print(
                f"Order ID : "
                f"{event.get('order_id')}"
            )

            print(
                f"Product  : "
                f"{event.get('product_name')}"
            )

            print(
                f"Category : "
                f"{event.get('category')}"
            )

            print(
                f"Quantity : "
                f"{event.get('quantity')}"
            )

            print(
                f"Amount   : "
                f"₹{event.get('total_amount', 0):,}"
            )

            print(
                f"Payment  : "
                f"{event.get('payment_method')}"
            )

            print(
                f"Status   : "
                f"{event.get('order_status')}"
            )

            print(
                f"Partition: "
                f"{message.partition}"
            )

            print(
                f"Offset   : "
                f"{message.offset}"
            )

            save_event(event)

    except KeyboardInterrupt:

        print(
            "\n\nConsumer stopped by user."
        )

    except Exception as error:

        print(
            f"\nConsumer failed: {error}"
        )

        raise

    finally:

        consumer.close()

        print(
            "Kafka consumer closed."
        )


if __name__ == "__main__":

    consume_events()