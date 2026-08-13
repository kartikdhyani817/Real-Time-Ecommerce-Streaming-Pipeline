import json
import time

from kafka import KafkaProducer

from producer.event_generator import generate_event


KAFKA_SERVER = "localhost:9092"

TOPIC_NAME = "ecommerce-orders"


def create_producer():

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,

        value_serializer=lambda value: json.dumps(
            value
        ).encode("utf-8"),

        acks="all",

        retries=3,
    )

    return producer


def stream_events(
    total_events=20,
    delay_seconds=1,
):

    print("=" * 60)
    print("E-Commerce Kafka Producer")
    print("=" * 60)

    print(
        f"\nConnecting to Kafka: {KAFKA_SERVER}"
    )

    print(
        f"Topic: {TOPIC_NAME}\n"
    )

    producer = create_producer()

    try:

        for event_number in range(
            1,
            total_events + 1,
        ):

            event = generate_event()

            future = producer.send(
                TOPIC_NAME,
                value=event,
            )

            metadata = future.get(
                timeout=10
            )

            print(
                f"[{event_number}/{total_events}] "
                f"SENT | "
                f"{event['order_id']} | "
                f"{event['product_name']} | "
                f"₹{event['total_amount']:,} | "
                f"Partition: {metadata.partition} | "
                f"Offset: {metadata.offset}"
            )

            time.sleep(
                delay_seconds
            )

    except KeyboardInterrupt:

        print(
            "\nProducer stopped by user."
        )

    except Exception as error:

        print(
            f"\nProducer failed: {error}"
        )

        raise

    finally:

        producer.flush()

        producer.close()

        print(
            "\nKafka producer closed."
        )


if __name__ == "__main__":

    stream_events(
        total_events=20,
        delay_seconds=1,
    )