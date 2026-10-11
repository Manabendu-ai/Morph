
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, sum as spark_sum, count, when
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)

KAFKA_SERVERS = "localhost:9092"
TOPIC = "morph.transactions"

CHECKPOINT = "/tmp/morph-spark-kafka-checkpoint"

transaction_schema = StructType([
    StructField("step", IntegerType(), True),
    StructField("type", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("nameOrig", StringType(), True),
    StructField("oldbalanceOrg", DoubleType(), True),
    StructField("newbalanceOrig", DoubleType(), True),
    StructField("nameDest", StringType(), True),
    StructField("oldbalanceDest", DoubleType(), True),
    StructField("newbalanceDest", DoubleType(), True),
    StructField("isFraud", IntegerType(), True),
    StructField("isFlaggedFraud", IntegerType(), True),
])


def process_batch(batch_df, batch_id):
    """Process and report one Spark micro-batch."""

    total = batch_df.count()

    if total == 0:
        print(f"\nBatch {batch_id}: no events")
        return

    valid_df = batch_df.filter(
        col("transaction").isNotNull()
        & col("transaction.type").isNotNull()
        & (col("transaction.amount") >= 0)
        & col("transaction.isFraud").isin(0, 1)
    )

    valid_count = valid_df.count()
    invalid_count = total - valid_count

    print("\n" + "=" * 65)
    print(f"MORPH | SPARK MICRO-BATCH {batch_id}")
    print("=" * 65)
    print(f"Events received : {total}")
    print(f"Valid events    : {valid_count}")
    print(f"Invalid events  : {invalid_count}")

    if valid_count == 0:
        return

    analytics = (
        valid_df
        .select("transaction.*")
        .groupBy("type")
        .agg(
            count("*").alias("transaction_count"),
            spark_sum("amount").alias("total_amount"),
            spark_sum(
                when(col("isFraud") == 1, 1).otherwise(0)
            ).alias("labelled_fraud_count"),
        )
        .orderBy("type")
    )

    analytics.show(truncate=False)


def main():
    spark = (
        SparkSession.builder
        .appName("MORPH-Kafka-Streaming")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print("MORPH Spark streaming processor starting...")
    print(f"Kafka broker: {KAFKA_SERVERS}")
    print(f"Kafka topic : {TOPIC}")

    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed_stream = raw_stream.select(
        from_json(
            col("value").cast("string"),
            transaction_schema,
        ).alias("transaction")
    )

    query = (
        parsed_stream.writeStream
        .foreachBatch(process_batch)
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT)
        .trigger(processingTime="5 seconds")
        .start()
    )

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\nStopping MORPH Spark processor...")
    finally:
        query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
