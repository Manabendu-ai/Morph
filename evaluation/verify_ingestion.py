
import csv
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

SOURCE = ROOT / "data/raw/PS_20174392719_1491204439457_log.csv"
CLEAN = ROOT / "data/processed/transactions_clean.csv"
REJECTED = ROOT / "data/processed/transactions_rejected.csv"
REPORT = ROOT / "data/processed/quality_report.json"


def inspect_csv(path):
    """Count records and inspect the CSV header without loading all rows."""
    count = 0
    fraud_counts = {"0": 0, "1": 0}
    reasons = {}

    with path.open("r", newline="", encoding="utf-8-sig") as file:
        reader = csv.DictReader(file)
        columns = reader.fieldnames

        if not columns:
            raise ValueError(f"Missing CSV header: {path}")

        for row in reader:
            count += 1

            if "isFraud" in row:
                fraud = row["isFraud"]
                if fraud in fraud_counts:
                    fraud_counts[fraud] += 1

            if "rejection_reason" in row:
                reason = row["rejection_reason"]
                reasons[reason] = reasons.get(reason, 0) + 1

    return count, columns, fraud_counts, reasons


def verify():
    for path in (SOURCE, CLEAN, REJECTED, REPORT):
        if not path.exists():
            raise FileNotFoundError(f"Missing output: {path}")

    source_count, source_columns, _, _ = inspect_csv(SOURCE)
    clean_count, clean_columns, clean_fraud, _ = inspect_csv(CLEAN)
    rejected_count, rejected_columns, _, reasons = inspect_csv(REJECTED)

    report = json.loads(REPORT.read_text(encoding="utf-8"))

    checks = {
        "Source has 6,362,620 records":
            source_count == 6_362_620,

        "Clean and rejected counts sum to source":
            clean_count + rejected_count == source_count,

        "Clean CSV preserves source columns":
            clean_columns == source_columns,

        "Rejected CSV adds rejection_reason":
            rejected_columns == source_columns + ["rejection_reason"],

        "Report source count matches":
            report["total_records"] == source_count,

        "Report valid count matches":
            report["valid_records"] == clean_count,

        "Report rejected count matches":
            report["rejected_records"] == rejected_count,

        "Report fraud counts match clean CSV":
            report["fraud_distribution"] == {
                "legitimate": clean_fraud["0"],
                "fraud": clean_fraud["1"],
            },

        "Report rejection reasons match rejected CSV":
            report["rejection_reasons"] == reasons,
    }

    print("\nMORPH | INGESTION INTEGRITY REPORT")
    print("=" * 55)
    print(f"Source records   : {source_count:,}")
    print(f"Clean records    : {clean_count:,}")
    print(f"Rejected records : {rejected_count:,}")
    print(f"Fraud labels     : {clean_fraud['1']:,}")
    print(f"Legitimate labels: {clean_fraud['0']:,}")

    print("\nVALIDATION CHECKS")
    print("-" * 55)

    for description, passed in checks.items():
        print(f"[{'PASS' if passed else 'FAIL'}] {description}")

    failures = [name for name, passed in checks.items() if not passed]

    print("\nRESULT:", "ALL CHECKS PASSED" if not failures else "CHECKS FAILED")

    if failures:
        raise SystemExit(
            "Resolve the failed checks before continuing to Kafka."
        )


if __name__ == "__main__":
    verify()
