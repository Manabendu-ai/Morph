
import csv
import json
from collections import Counter
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent

DATASET_PATH = (
    PROJECT_ROOT
    / "data"
    / "raw"
    / "PS_20174392719_1491204439457_log.csv"
)

OUTPUT_DIR = PROJECT_ROOT / "data" / "processed"

CLEAN_PATH = OUTPUT_DIR / "transactions_clean.csv"
REJECTED_PATH = OUTPUT_DIR / "transactions_rejected.csv"
REPORT_PATH = OUTPUT_DIR / "quality_report.json"

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

NUMERIC_COLUMNS = {
    "step",
    "amount",
    "oldbalanceOrg",
    "newbalanceOrig",
    "oldbalanceDest",
    "newbalanceDest",
    "isFraud",
    "isFlaggedFraud",
}


def validate_transaction(row):
    """Return a rejection reason, or None for a valid record."""

    for column in REQUIRED_COLUMNS:
        value = row.get(column)

        if value is None or not value.strip():
            return f"Missing value: {column}"

    try:
        step = int(row["step"])
        amount = float(row["amount"])

        old_origin = float(row["oldbalanceOrg"])
        new_origin = float(row["newbalanceOrig"])
        old_dest = float(row["oldbalanceDest"])
        new_dest = float(row["newbalanceDest"])

        fraud = int(row["isFraud"])
        flagged = int(row["isFlaggedFraud"])

        numeric_values = [
            amount, old_origin, new_origin, old_dest, new_dest
        ]

        # Reject NaN and infinity.
        import math
        if not all(math.isfinite(value) for value in numeric_values):
            return "Non-finite numeric value"

        if step < 0:
            return "Negative transaction step"

        if amount < 0:
            return "Negative transaction amount"

        if min(old_origin, new_origin, old_dest, new_dest) < 0:
            return "Negative account balance"

        if fraud not in (0, 1) or flagged not in (0, 1):
            return "Invalid fraud flag"

        if not row["type"].strip():
            return "Missing transaction type"

    except (ValueError, TypeError, OverflowError):
        return "Invalid numeric field"

    return None


def process_dataset():
    if not DATASET_PATH.exists():
        raise FileNotFoundError(
            f"Dataset not found: {DATASET_PATH}"
        )

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    total = 0
    valid = 0
    rejected = 0
    fraud_counts = Counter()
    transaction_types = Counter()
    rejection_reasons = Counter()
    total_amount = 0.0

    print("MORPH | Data Ingestion and Quality Pipeline")
    print("=" * 55)

    with (
        DATASET_PATH.open(
            "r", newline="", encoding="utf-8-sig"
        ) as source,
        CLEAN_PATH.open(
            "w", newline="", encoding="utf-8"
        ) as clean_file,
        REJECTED_PATH.open(
            "w", newline="", encoding="utf-8"
        ) as rejected_file,
    ):
        reader = csv.DictReader(source)

        if not reader.fieldnames:
            raise ValueError("CSV file has no header")

        missing_columns = REQUIRED_COLUMNS - set(reader.fieldnames)

        if missing_columns:
            raise ValueError(
                f"Missing required columns: {sorted(missing_columns)}"
            )

        clean_writer = csv.DictWriter(
            clean_file,
            fieldnames=reader.fieldnames,
        )
        clean_writer.writeheader()

        rejected_writer = csv.DictWriter(
            rejected_file,
            fieldnames=reader.fieldnames + ["rejection_reason"],
        )
        rejected_writer.writeheader()

        for row in reader:
            total += 1
            reason = validate_transaction(row)

            if reason:
                rejected += 1
                rejection_reasons[reason] += 1

                rejected_writer.writerow({
                    **row,
                    "rejection_reason": reason,
                })
                continue

            clean_writer.writerow(row)
            valid += 1

            fraud_counts[int(row["isFraud"])] += 1
            transaction_types[row["type"]] += 1
            total_amount += float(row["amount"])

            if total % 1_000_000 == 0:
                print(f"Processed {total:,} records")

    report = {
        "source_file": DATASET_PATH.name,
        "total_records": total,
        "valid_records": valid,
        "rejected_records": rejected,
        "rejection_reasons": dict(rejection_reasons),
        "fraud_distribution": {
            "legitimate": fraud_counts[0],
            "fraud": fraud_counts[1],
        },
        "transaction_types": dict(transaction_types),
        "valid_transaction_amount_total": round(total_amount, 2),
        "data_quality_percentage": round(
            valid / total * 100, 4
        ) if total else 0,
    }

    REPORT_PATH.write_text(
        json.dumps(report, indent=4),
        encoding="utf-8",
    )

    print("\nMORPH INGESTION COMPLETED")
    print("=" * 55)
    print(f"Total records    : {total:,}")
    print(f"Valid records    : {valid:,}")
    print(f"Rejected records : {rejected:,}")
    print(f"Quality score    : {report['data_quality_percentage']}%")
    print(f"Clean data       : {CLEAN_PATH}")
    print(f"Rejected data    : {REJECTED_PATH}")
    print(f"Quality report   : {REPORT_PATH}")


if __name__ == "__main__":
    process_dataset()
