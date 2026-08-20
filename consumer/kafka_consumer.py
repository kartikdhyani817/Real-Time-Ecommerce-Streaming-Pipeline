import json
from pathlib import Path

from kafka import KafkaConsumer

from scripts.event_validator import validate_event
from scripts.parquet_writer import save_event_to_parquet
from utils.logger import setup_logger


logger = setup_logger(
    "KafkaConsumer"
)

KAFKA_SERVER = "localhost:9092"
TOPIC_NAME = "ecommerce-orders"
GROUP_ID = "ecommerce-validation-group"

OUTPUT_DIR = Path("data/processed")

VALID_FILE = OUTPUT_DIR / "valid_events.jsonl"
INVALID_FILE = OUTPUT_DIR / "invalid_events.jsonl"


def create_consumer():
    return KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=KAFKA_SERVER,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda message: json.loads(
            message.decode("utf-8")
        ),
    )


def save_json_line(file_path, data):
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
    logger.info(
        "Kafka consumer started."
    )
    print("=" * 65)
    print("E-Commerce Kafka Validation Consumer")
    print("=" * 65)

    print(f"\nKafka Server   : {KAFKA_SERVER}")
    print(f"Topic          : {TOPIC_NAME}")
    print(f"Consumer Group : {GROUP_ID}")
    print("\nWaiting for events...\n")

    consumer = create_consumer()

    total_events = 0
    valid_events = 0
    invalid_events = 0

    try:
        for message in consumer:
            total_events += 1

            event = message.value

            is_valid, errors = validate_event(event)

            print("-" * 65)
            print(f"EVENT #{total_events}")
            print(f"Partition : {message.partition}")
            print(f"Offset    : {message.offset}")

            if is_valid:
                valid_events += 1

                # Save valid event as JSONL backup
                save_json_line(
                    VALID_FILE,
                    event,
                )

                # Save valid event as Parquet
                parquet_file = save_event_to_parquet(
                    event
                )
                logger.info(
                    f"Valid event processed | "
                    f"order_id={event.get('order_id')} | "
                    f"event_id={event.get('event_id')} | "
                    f"amount={event.get('total_amount')} | "
                    f"partition={message.partition} | "
                    f"offset={message.offset}"
                )

                print("Status    : VALID")
                print(
                    f"Order ID  : "
                    f"{event.get('order_id')}"
                )
                print(
                    f"Product   : "
                    f"{event.get('product_name')}"
                )
                print(
                    f"Category  : "
                    f"{event.get('category')}"
                )
                print(
                    f"Quantity  : "
                    f"{event.get('quantity')}"
                )
                print(
                    f"Amount    : "
                    f"₹{event.get('total_amount', 0):,}"
                )
                print(
                    f"Payment   : "
                    f"{event.get('payment_method')}"
                )
                print(
                    f"Stored    : "
                    f"{parquet_file}"
                )

            else:
                invalid_events += 1

                invalid_record = {
                    "event": event,
                    "validation_errors": errors,
                    "kafka_partition": message.partition,
                    "kafka_offset": message.offset,
                }

                save_json_line(
                    INVALID_FILE,
                    invalid_record,
                )
                logger.warning(
                    f"Invalid event detected | "
                    f"event_id={event.get('event_id')} | "
                    f"errors={errors} | "
                    f"partition={message.partition} | "
                    f"offset={message.offset}"
                )

                print("Status    : INVALID")
                print("Validation Errors:")

                for error in errors:
                    print(f"  - {error}")

            print("\nStream Summary")
            print(f"Total   : {total_events}")
            print(f"Valid   : {valid_events}")
            print(f"Invalid : {invalid_events}")

    except KeyboardInterrupt:
        print("\nConsumer stopped by user.")
        logger.info(
            "Kafka consumer stopped by user."
        )

    except Exception as error:

        logger.exception(
            f"Consumer failed: {error}"
        )

        print(
            f"\nConsumer failed: {error}"
        )
        raise

    finally:
        consumer.close()
        print("\nKafka consumer closed.")
        logger.info(
            "Kafka consumer connection closed."
        )


if __name__ == "__main__":
    consume_events()