
import json

from kafka import KafkaConsumer
from kafka.errors import KafkaError

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "morph.transactions"


def main():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        group_id=None,
        key_deserializer=lambda key: (
            key.decode("utf-8") if key else None
        ),
        value_deserializer=lambda value: json.loads(
            value.decode("utf-8")
        ),
        consumer_timeout_ms=10_000,
    )

    received = 0

    print(f"Listening to topic: {TOPIC}")
    print("Waiting for transaction events...\n")

    try:
        for message in consumer:
            transaction = message.value

            print(
                f"Event #{received + 1} | "
                f"partition={message.partition} | "
                f"offset={message.offset}"
            )
            print(f"  Account : {message.key}")
            print(f"  Type    : {transaction['type']}")
            print(f"  Amount  : {transaction['amount']}")
            print(f"  Fraud   : {transaction['isFraud']}")
            print("-" * 40)

            received += 1

            if received >= 10:
                break

    except (KafkaError, ValueError, KeyError) as error:
        print(f"Consumer failed: {error}")
        raise

    finally:
        consumer.close()

    print(f"\nConsumer verification complete: {received} events received.")


if __name__ == "__main__":
    main()
