
import csv
from collections import Counter
from pathlib import Path

# Project root: the parent of the ingestion directory
PROJECT_ROOT = Path(__file__).resolve().parent.parent

DATASET_PATH = (
    PROJECT_ROOT
    / "data"
    / "raw"
    / "PS_20174392719_1491204439457_log.csv"
)

REQUIRED_COLUMNS = {
    "step",
    "type",
    "amount",
    "nameOrig",
    "oldbalanceOrg",
    "newbalanceOrig",
    "nameDest",
    "oldbalanceDest",
    "newbalanceDest",
    "isFraud",
    "isFlaggedFraud",
}


def inspect_dataset():
    if not DATASET_PATH.exists():
        raise FileNotFoundError(
            f"Dataset not found: {DATASET_PATH}\n"
            "Download PaySim and place its CSV in data/raw/."
        )

    transaction_types = Counter()
    fraud_counts = Counter()

    total_rows = 0
    invalid_rows = 0
    missing_values = 0
    total_amount = 0.0
    max_amount = 0.0

    print("MORPH | Transaction Ingestion")
    print("=" * 50)
    print(f"Dataset: {DATASET_PATH.name}")
    print("Reading transactions...")

    # Read incrementally instead of loading millions of rows at once.
    with DATASET_PATH.open(
        "r", newline="", encoding="utf-8-sig"
    ) as file:
        reader = csv.DictReader(file)

        if reader.fieldnames is None:
            raise ValueError("The CSV file has no header.")

        missing_columns = REQUIRED_COLUMNS - set(reader.fieldnames)

        if missing_columns:
            raise ValueError(
                f"Missing required columns: {sorted(missing_columns)}"
            )

        for row in reader:
            total_rows += 1

            if any(
                row.get(column) is None
                or row[column].strip() == ""
                for column in REQUIRED_COLUMNS
            ):
                missing_values += 1
                invalid_rows += 1
                continue

            try:
                amount = float(row["amount"])
                int(row["step"])
                float(row["oldbalanceOrg"])
                float(row["newbalanceOrig"])
                float(row["oldbalanceDest"])
                float(row["newbalanceDest"])

                fraud = int(row["isFraud"])
                flagged = int(row["isFlaggedFraud"])

                if (
                    amount < 0
                    or fraud not in (0, 1)
                    or flagged not in (0, 1)
                ):
                    invalid_rows += 1
                    continue

            except (ValueError, TypeError):
                invalid_rows += 1
                continue

            transaction_types[row["type"]] += 1
            fraud_counts[fraud] += 1
            total_amount += amount
            max_amount = max(max_amount, amount)

            if total_rows % 1_000_000 == 0:
                print(f"Scanned {total_rows:,} records...")

    valid_rows = sum(fraud_counts.values())

    print("\nINGESTION SUMMARY")
    print("-" * 50)
    print(f"Total records scanned : {total_rows:,}")
    print(f"Valid records         : {valid_rows:,}")
    print(f"Invalid records       : {invalid_rows:,}")
    print(f"Records with missing values: {missing_values:,}")

    print("\nTRANSACTION TYPES")
    for transaction_type, count in transaction_types.most_common():
        print(f"{transaction_type:15} {count:>12,}")

    print("\nFRAUD DISTRIBUTION")
    print(f"Legitimate transactions: {fraud_counts[0]:,}")
    print(f"Fraud transactions     : {fraud_counts[1]:,}")

    if valid_rows:
        print("\nTRANSACTION STATISTICS")
        print(f"Total transaction amount: {total_amount:,.2f}")
        print(f"Average amount          : {total_amount / valid_rows:,.2f}")
        print(f"Largest amount          : {max_amount:,.2f}")

    print("\nMORPH ingestion inspection completed.")


if __name__ == "__main__":
    inspect_dataset()
