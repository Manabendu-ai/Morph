
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    window,
    count,
    sum as spark_sum,
    when,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)

KAFKA_SERVERS = "localhost:9092"
TOPIC = "morph.transactions.v2"

# This query gets its own checkpoint.
CHECKPOINT = "/tmp/morph-spark-windowed-checkpoint"

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
    StructField("event_id", StringType(), True),
])


def main():
    spark = (
        SparkSession.builder
        .appName("MORPH-Windowed-Analytics")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print("MORPH | REAL-TIME WINDOWED ANALYTICS")
    print(f"Kafka topic: {TOPIC}")
    print("Window duration: 10 seconds")

    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = raw_stream.select(
        col("timestamp").alias("event_time"),
        from_json(
            col("value").cast("string"),
            transaction_schema,
        ).alias("transaction"),
    )

    transactions = (
        parsed
        .filter(
            col("transaction").isNotNull()
            & col("transaction.type").isNotNull()
            & col("transaction.amount").isNotNull()
            & (col("transaction.amount") >= 0)
            & col("transaction.isFraud").isin(0, 1)
        )
        .select(
            "event_time",
            col("transaction.type").alias("transaction_type"),
            col("transaction.amount").alias("amount"),
            col("transaction.isFraud").alias("is_fraud"),
        )
    )

    # Keep event-time state bounded.
    windowed = (
        transactions
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
        windowed.writeStream
        .format("console")
        .outputMode("update")
        .option("truncate", "false")
        .option("numRows", "100")
        .option("checkpointLocation", CHECKPOINT)
        .trigger(processingTime="5 seconds")
        .start()
    )

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\nStopping MORPH windowed analytics...")
    finally:
        query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
