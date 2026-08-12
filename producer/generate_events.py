import json
import time
from pathlib import Path

from producer.event_generator import generate_event


OUTPUT_FOLDER = Path("data/raw")

OUTPUT_FILE = OUTPUT_FOLDER / "events.jsonl"


def save_event(event):
    """
    Append one JSON event to the raw event file.
    """

    OUTPUT_FOLDER.mkdir(
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


def generate_events(
    total_events=20,
    delay_seconds=0.5,
):
    """
    Generate multiple simulated e-commerce events.
    """

    print("=" * 60)
    print("E-Commerce Event Generator")
    print("=" * 60)

    print(
        f"\nGenerating {total_events} events...\n"
    )

    for event_number in range(
        1,
        total_events + 1,
    ):

        event = generate_event()

        save_event(event)

        print(
            f"[{event_number}/{total_events}] "
            f"{event['order_id']} | "
            f"{event['product_name']} | "
            f"Qty: {event['quantity']} | "
            f"Amount: ₹{event['total_amount']:,}"
        )

        time.sleep(
            delay_seconds
        )

    print(
        "\nEvent generation completed successfully."
    )

    print(
        f"Events saved to: {OUTPUT_FILE}"
    )


if __name__ == "__main__":

    generate_events(
        total_events=20,
        delay_seconds=0.2,
    )
