
import os

import psycopg
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, count, sum as spark_sum, when
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

KAFKA_SERVERS = "localhost:9092"
TOPIC = "morph.transactions.v2"
CHECKPOINT = "/tmp/morph-spark-storage-checkpoint"

DB_NAME = os.getenv("MORPH_DB_NAME", "morph")
DB_USER = os.getenv("MORPH_DB_USER", "postgres")
DB_PASSWORD = os.getenv("MORPH_DB_PASSWORD")
DB_HOST = os.getenv("MORPH_DB_HOST", "localhost")
DB_PORT = int(os.getenv("MORPH_DB_PORT", "5432"))

schema = StructType([
    StructField("step", IntegerType()),
    StructField("type", StringType()),
    StructField("amount", DoubleType()),
    StructField("nameOrig", StringType()),
    StructField("oldbalanceOrg", DoubleType()),
    StructField("newbalanceOrig", DoubleType()),
    StructField("nameDest", StringType()),
    StructField("oldbalanceDest", DoubleType()),
    StructField("newbalanceDest", DoubleType()),
    StructField("isFraud", IntegerType()),
    StructField("isFlaggedFraud", IntegerType()),
    StructField("event_id", StringType()),
])


def save_batch(batch_df, batch_id):
    rows = batch_df.collect()

    if not rows:
        print(f"Batch {batch_id}: no completed windows")
        return

    sql = """
        INSERT INTO transaction_window_metrics (
            window_start, window_end, transaction_type,
            transaction_count, total_amount, labelled_fraud_count,
            updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (window_start, window_end, transaction_type)
        DO UPDATE SET
            transaction_count = EXCLUDED.transaction_count,
            total_amount = EXCLUDED.total_amount,
            labelled_fraud_count = EXCLUDED.labelled_fraud_count,
            updated_at = NOW()
    """

    with psycopg.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
    ) as connection:
        with connection.cursor() as cursor:
            for row in rows:
                cursor.execute(
                    sql,
                    (
                        row.window.start,
                        row.window.end,
                        row.transaction_type,
                        row.transaction_count,
                        row.total_amount,
                        row.labelled_fraud_count,
                    ),
                )

    print(f"Batch {batch_id}: saved {len(rows)} window metrics")


def main():
    if not DB_PASSWORD:
        raise RuntimeError("Set MORPH_DB_PASSWORD before starting.")

    spark = (
        SparkSession.builder
        .appName("MORPH-Windowed-PostgreSQL")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    parsed = raw.select(
        col("timestamp").alias("event_time"),
        from_json(col("value").cast("string"), schema).alias("tx"),
    )

    valid = parsed.filter(
        col("tx").isNotNull()
        & col("tx.type").isNotNull()
        & col("tx.amount").isNotNull()
        & (col("tx.amount") >= 0)
        & col("tx.isFraud").isin(0, 1)
    ).select(
        "event_time",
        col("tx.type").alias("transaction_type"),
        col("tx.amount").alias("amount"),
        col("tx.isFraud").alias("is_fraud"),
    )

    metrics = (
        valid
        .withWatermark("event_time", "30 seconds")
        .groupBy(
            window(col("event_time"), "10 seconds"),
            col("transaction_type"),
        )
        .agg(
            count("*").alias("transaction_count"),
            spark_sum("amount").alias("total_amount"),
            spark_sum(
                when(col("is_fraud") == 1, 1).otherwise(0)
            ).alias("labelled_fraud_count"),
        )
    )

    query = (
        metrics.writeStream
        .outputMode("update")
        .foreachBatch(save_batch)
        .option("checkpointLocation", CHECKPOINT)
        .trigger(processingTime="5 seconds")
        .start()
    )

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("Stopping MORPH storage processor")
    finally:
        query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
