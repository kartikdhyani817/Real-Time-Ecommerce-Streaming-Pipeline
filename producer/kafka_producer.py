import json
import time

from kafka import KafkaProducer

from producer.event_generator import generate_event
from utils.logger import setup_logger


# ============================================================
# CONFIGURATION
# ============================================================

KAFKA_SERVER = "localhost:9092"
TOPIC_NAME = "ecommerce-orders"

NUMBER_OF_EVENTS = 10
EVENT_DELAY = 1


# ============================================================
# LOGGER
# ============================================================

logger = setup_logger(
    "KafkaProducer"
)


# ============================================================
# CREATE KAFKA PRODUCER
# ============================================================

def create_producer():
    """
    Create and return Kafka producer.
    """

    return KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,

        value_serializer=lambda value: (
            json.dumps(value)
            .encode("utf-8")
        ),
    )


# ============================================================
# PRODUCE EVENTS
# ============================================================

def produce_events():
    """
    Generate e-commerce events and publish them to Kafka.
    """

    print("=" * 65)
    print("E-Commerce Kafka Producer")
    print("=" * 65)

    print(
        f"\nKafka Server : {KAFKA_SERVER}"
    )

    print(
        f"Topic        : {TOPIC_NAME}"
    )

    print(
        f"Events       : {NUMBER_OF_EVENTS}"
    )

    print(
        "\nStarting producer...\n"
    )

    logger.info(
        "Kafka producer started."
    )

    producer = None

    try:

        producer = create_producer()

        for event_number in range(
            1,
            NUMBER_OF_EVENTS + 1,
        ):

            event = generate_event()

            future = producer.send(
                TOPIC_NAME,
                value=event,
            )

            metadata = future.get(
                timeout=10
            )

            print("-" * 65)

            print(
                f"EVENT #{event_number}"
            )

            print(
                f"Event ID  : "
                f"{event.get('event_id')}"
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
                f"Partition : "
                f"{metadata.partition}"
            )

            print(
                f"Offset    : "
                f"{metadata.offset}"
            )

            logger.info(
                f"Event published | "
                f"event_id={event.get('event_id')} | "
                f"order_id={event.get('order_id')} | "
                f"topic={TOPIC_NAME} | "
                f"partition={metadata.partition} | "
                f"offset={metadata.offset}"
            )

            time.sleep(
                EVENT_DELAY
            )

        producer.flush()

        print(
            "\nAll events published successfully."
        )

        logger.info(
            f"Kafka producer finished successfully | "
            f"events_published={NUMBER_OF_EVENTS}"
        )

    except KeyboardInterrupt:

        print(
            "\nProducer stopped by user."
        )

        logger.info(
            "Kafka producer stopped by user."
        )

    except Exception as error:

        print(
            f"\nProducer failed: {error}"
        )

        logger.exception(
            f"Producer failed: {error}"
        )

        raise

    finally:

        if producer is not None:

            producer.close()

            logger.info(
                "Kafka producer connection closed."
            )


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":

    produce_events()