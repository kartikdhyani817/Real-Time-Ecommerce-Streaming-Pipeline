import json
from pathlib import Path

from kafka import KafkaConsumer

from scripts.event_validator import (
    validate_event,
)


KAFKA_SERVER = "localhost:9092"

TOPIC_NAME = "ecommerce-orders"

GROUP_ID = "ecommerce-validation-group"


OUTPUT_DIR = Path(
    "data/processed"
)

VALID_FILE = (
    OUTPUT_DIR
    / "valid_events.jsonl"
)

INVALID_FILE = (
    OUTPUT_DIR
    / "invalid_events.jsonl"
)


def create_consumer():
    """
    Create Kafka consumer.
    """

    consumer = KafkaConsumer(
        TOPIC_NAME,

        bootstrap_servers=KAFKA_SERVER,

        group_id=GROUP_ID,

        auto_offset_reset="earliest",

        enable_auto_commit=True,

        value_deserializer=lambda message: (
            json.loads(
                message.decode("utf-8")
            )
        ),
    )

    return consumer


def save_json_line(
    file_path,
    data,
):
    """
    Append a JSON record to a JSONL file.
    """

    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    with file_path.open(
        "a",
        encoding="utf-8",
    ) as file:

        file.write(
            json.dumps(
                data,
                ensure_ascii=False,
            )
            + "\n"
        )


def consume_events():
    """
    Consume, validate and route Kafka events.
    """

    print("=" * 65)
    print(
        "E-Commerce Kafka Validation Consumer"
    )
    print("=" * 65)

    print(
        f"\nKafka Server  : "
        f"{KAFKA_SERVER}"
    )

    print(
        f"Topic         : "
        f"{TOPIC_NAME}"
    )

    print(
        f"Consumer Group: "
        f"{GROUP_ID}"
    )

    print(
        "\nWaiting for events...\n"
    )

    consumer = create_consumer()

    total_events = 0
    valid_events = 0
    invalid_events = 0

    try:

        for message in consumer:

            total_events += 1

            event = message.value

            is_valid, errors = (
                validate_event(event)
            )

            print("-" * 65)

            print(
                f"EVENT #{total_events}"
            )

            print(
                f"Partition : "
                f"{message.partition}"
            )

            print(
                f"Offset    : "
                f"{message.offset}"
            )

            # ---------------------------------
            # VALID EVENT
            # ---------------------------------

            if is_valid:

                valid_events += 1

                save_json_line(
                    VALID_FILE,
                    event,
                )

                print(
                    "Status    : VALID"
                )

                print(
                    f"Order ID  : "
                    f"{event.get('order_id')}"
                )

                print(
                    f"Product   : "
                    f"{event.get('product_name')}"
                )

                print(
                    f"Amount    : "
                    f"₹{event.get('total_amount', 0):,}"
                )

            # ---------------------------------
            # INVALID EVENT
            # ---------------------------------

            else:

                invalid_events += 1

                invalid_record = {
                    "event": event,
                    "validation_errors": errors,
                    "kafka_partition": (
                        message.partition
                    ),
                    "kafka_offset": (
                        message.offset
                    ),
                }

                save_json_line(
                    INVALID_FILE,
                    invalid_record,
                )

                print(
                    "Status    : INVALID"
                )

                print(
                    "Validation Errors:"
                )

                for error in errors:

                    print(
                        f"  - {error}"
                    )

            # ---------------------------------
            # STREAM SUMMARY
            # ---------------------------------

            print(
                "\nStream Summary"
            )

            print(
                f"Total   : {total_events}"
            )

            print(
                f"Valid   : {valid_events}"
            )

            print(
                f"Invalid : {invalid_events}"
            )

    except KeyboardInterrupt:

        print(
            "\nConsumer stopped by user."
        )

    except Exception as error:

        print(
            f"\nConsumer failed: {error}"
        )

        raise

    finally:

        consumer.close()

        print(
            "\nKafka consumer closed."
        )


if __name__ == "__main__":

    consume_events()