
import csv
import json
import time
from pathlib import Path

from kafka import KafkaProducer
from kafka.errors import KafkaError

ROOT = Path(__file__).resolve().parent.parent
CSV_PATH = ROOT / "data/processed/transactions_clean.csv"

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "morph.transactions"

# Start small. Increase only after successful verification.
MAX_MESSAGES = 1_000
MESSAGES_PER_SECOND = 100


def convert_transaction(row):
    """Convert CSV strings into typed JSON transaction fields."""

    integer_fields = {"step", "isFraud", "isFlaggedFraud"}
    float_fields = {
        "amount",
        "oldbalanceOrg",
        "newbalanceOrig",
        "oldbalanceDest",
        "newbalanceDest",
    }

    event = {}

    for key, value in row.items():
        if key in integer_fields:
            event[key] = int(value)
        elif key in float_fields:
            event[key] = float(value)
        else:
            event[key] = value

    return event


def main():
    if not CSV_PATH.exists():
        raise FileNotFoundError(f"Clean dataset not found: {CSV_PATH}")

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda value: json.dumps(
            value, allow_nan=False
        ).encode("utf-8"),
        acks="all",
        retries=5,
    )

    sent = 0
    started = time.monotonic()
    interval = 1.0 / MESSAGES_PER_SECOND

    print(f"Publishing to Kafka topic: {TOPIC}")

    try:
        with CSV_PATH.open(
            "r", newline="", encoding="utf-8-sig"
        ) as file:
            reader = csv.DictReader(file)

            for row in reader:
                if sent >= MAX_MESSAGES:
                    break

                event = convert_transaction(row)

                # Use the origin account as the Kafka message key.
                future = producer.send(
                    TOPIC,
                    key=event["nameOrig"],
                    value=event,
                )

                # Wait for broker acknowledgement.
                metadata = future.get(timeout=30)

                sent += 1

                if sent % 100 == 0:
                    print(
                        f"Sent {sent} events | "
                        f"partition={metadata.partition} | "
                        f"offset={metadata.offset}"
                    )

                # Throttle the initial test to approximately 100/sec.
                target_elapsed = sent * interval
                elapsed = time.monotonic() - started

                if target_elapsed > elapsed:
                    time.sleep(target_elapsed - elapsed)

        producer.flush()

        print(f"\nSuccessfully acknowledged {sent} events.")
        print(f"Elapsed time: {time.monotonic() - started:.2f}s")

    except (KafkaError, OSError, ValueError, KeyError) as error:
        print(f"Producer failed: {error}")
        raise

    finally:
        producer.close()


if __name__ == "__main__":
    main()
